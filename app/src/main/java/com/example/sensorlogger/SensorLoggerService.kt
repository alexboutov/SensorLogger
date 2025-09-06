package com.example.sensorlogger

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Context
import android.content.Intent
import android.hardware.Sensor
import android.hardware.SensorEvent
import android.hardware.SensorEventListener
import android.hardware.SensorManager
import android.os.IBinder
import android.os.PowerManager
import androidx.core.app.NotificationCompat
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import java.io.File
import java.io.FileWriter
import java.io.Writer
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.Date
import java.util.concurrent.Executors
import kotlin.math.cos
import kotlin.math.sin
import kotlin.math.sqrt

class SensorLoggerService : Service(), SensorEventListener {

    private val sensorManager by lazy { getSystemService(Context.SENSOR_SERVICE) as SensorManager }

    private lateinit var logDir: File
    private lateinit var sessionTimestamp: String

    private val fileWriters = mutableMapOf<Int, FileWriter>()
    private var combinedWriter: FileWriter? = null

    // Buffers for latest readings (sensor timestamp + 3 components)
    private data class Snapshot(var ts: Long = 0L, val v: FloatArray = floatArrayOf(Float.NaN, Float.NaN, Float.NaN))
    private val latestLA = Snapshot()
    private val latestACC = Snapshot()
    private val latestGRAV = Snapshot()
    private val latestMAG = Snapshot()

    // Rotation providers
    private val gravity = FloatArray(3)
    private val magnetic = FloatArray(3)
    private var hasGravity = false
    private var hasMagnetic = false

    private val rotMatrixGM = FloatArray(9)
    private val rotMatrixRV = FloatArray(9)
    private val orientationValsGM = FloatArray(3) // [azimuth(Z), pitch(X), roll(Y)]
    private val orientationValsRV = FloatArray(3)

    // Rotation Vector (gyro-aided)
    private var hasRV = false
    private var latestRotTsNs: Long = 0L
    private var rotVec: FloatArray = FloatArray(5) // allocate max, handle short vectors safely

    // Projection direction from UI azimuth (deg clockwise from North).
    // World frame: X=East, Y=North, Z=Up.
    private var azimuthDeg: Double = 0.0
    private var dirFx: Double = 0.0   // East component = sin(az)
    private var dirFy: Double = 1.0   // North component = cos(az)

    // P components and accumulators
    private var latestP_gm: Double = 0.0
    private var latestP_rv: Double = 0.0
    private var latestP_accGM: Double = 0.0

    private var vAccum_gm: Double = 0.0     // integrates P_gm
    private var vAccum_rv: Double = 0.0     // integrates P_rv
    private var vAccum_accGM: Double = 0.0  // integrates P_accGM

    /** ===== Concurrency & lifecycle guards ===== */
    private val ioErrorHandler = CoroutineExceptionHandler { _, e ->
        android.util.Log.d("SensorLogger", "I/O after stop (ignored): ${e.message}")
    }
    private val serviceJob = SupervisorJob()
    // Single-threaded dispatcher to keep event order deterministic
    private val writeExecutor = Executors.newSingleThreadExecutor()
    private val ioScope = CoroutineScope(writeExecutor.asCoroutineDispatcher() + serviceJob + ioErrorHandler)

    @Volatile private var isLogging = false
    @Volatile private var writersOpen = false

    private lateinit var wakeLock: PowerManager.WakeLock

    // LA timing
    private var prevLaTsNs: Long = 0L           // last raw sensor ns
    private var laClockSec: Double = 0.0        // accumulated SECONDS since START

    // Simple lock for consistency (used inside single-thread too for clarity)
    private val lock = Any()

    override fun onCreate() {
        super.onCreate()
        setupForegroundNotification()
        acquireWakeLock()

        sessionTimestamp = SimpleDateFormat("yyyyMMdd_HHmmss", Locale.US).format(Date())
        logDir = File(getExternalFilesDir(null), "sensor_logs").apply { mkdirs() }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        // START
        isLogging = true
        writersOpen = true

        // Read azimuth from UI and precompute direction vector (X=East, Y=North)
        azimuthDeg = intent?.getDoubleExtra(EXTRA_AZIMUTH_DEG, 0.0) ?: 0.0
        val azRad = Math.toRadians(azimuthDeg)
        dirFx = sin(azRad) // East
        dirFy = cos(azRad) // North

        // Reset integrators/clocks
        vAccum_gm = 0.0
        vAccum_rv = 0.0
        vAccum_accGM = 0.0
        prevLaTsNs = 0L
        laClockSec = 0.0

        registerSensors()
        return START_STICKY
    }

    override fun onDestroy() {
        isLogging = false
        unregisterSensors()
        serviceJob.cancelChildren()
        closeWriters()
        writersOpen = false
        releaseWakeLock()
        writeExecutor.shutdown()
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun setupForegroundNotification() {
        val channelId = "SensorLoggerChannel"
        val mgr = getSystemService(NotificationManager::class.java)
        if (mgr.getNotificationChannel(channelId) == null) {
            val channel = NotificationChannel(channelId, "Sensor Logger", NotificationManager.IMPORTANCE_LOW)
            mgr.createNotificationChannel(channel)
        }

        val notification: Notification = NotificationCompat.Builder(this, channelId)
            .setContentTitle("Sensor Logging Active")
            .setSmallIcon(android.R.drawable.ic_menu_compass)
            .setOngoing(true)
            .build()

        startForeground(1, notification)
    }

    private fun acquireWakeLock() {
        val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
        wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "$packageName::SensorLoggerLock")
        wakeLock.acquire()
    }

    private fun releaseWakeLock() {
        if (::wakeLock.isInitialized && wakeLock.isHeld) {
            wakeLock.release()
        }
    }

    private fun registerSensors() {
        listOf(
            Sensor.TYPE_LINEAR_ACCELERATION,
            Sensor.TYPE_ACCELEROMETER,
            Sensor.TYPE_GRAVITY,
            Sensor.TYPE_MAGNETIC_FIELD,
            Sensor.TYPE_ROTATION_VECTOR
        ).forEach { type ->
            sensorManager.getDefaultSensor(type)?.let {
                sensorManager.registerListener(this, it, SensorManager.SENSOR_DELAY_FASTEST)
            }
        }
    }

    private fun unregisterSensors() {
        sensorManager.unregisterListener(this)
    }

    override fun onSensorChanged(event: SensorEvent) {
        if (!isLogging) return

        val sensorType = event.sensor.type
        val sysTimestamp = System.currentTimeMillis()
        val sensorTsNs = event.timestamp // monotonic, ns since boot
        val values = event.values.copyOf()

        when (sensorType) {
            Sensor.TYPE_GRAVITY -> {
                System.arraycopy(values, 0, gravity, 0, 3)
                hasGravity = true
                latestGRAV.ts = sensorTsNs
                System.arraycopy(values, 0, latestGRAV.v, 0, 3)
            }
            Sensor.TYPE_MAGNETIC_FIELD -> {
                System.arraycopy(values, 0, magnetic, 0, 3)
                hasMagnetic = true
                latestMAG.ts = sensorTsNs
                System.arraycopy(values, 0, latestMAG.v, 0, 3)
            }
            Sensor.TYPE_ROTATION_VECTOR -> {
                latestRotTsNs = sensorTsNs
                if (rotVec.size != values.size) {
                    // Keep rotVec capacity >= incoming vector length
                    rotVec = FloatArray(maxOf(5, values.size))
                }
                // Copy only the provided components
                for (i in values.indices) rotVec[i] = values[i]
                hasRV = true
            }
        }

        ioScope.launch {
            if (!isLogging || !writersOpen) return@launch

            synchronized(lock) {
                when (sensorType) {
                    Sensor.TYPE_LINEAR_ACCELERATION -> {
                        // ---- Update LA timing (sensor clock) ----
                        val prevForLine = prevLaTsNs
                        val curr = sensorTsNs
                        val dtNs = if (prevForLine == 0L) 0L else (curr - prevForLine).coerceAtLeast(0L)
                        val dtSec = if (prevForLine == 0L) 0.0 else dtNs.toDouble() * 1e-9
                        laClockSec += dtSec
                        prevLaTsNs = curr

                        // Update latest LA snapshot
                        latestLA.ts = curr
                        System.arraycopy(values, 0, latestLA.v, 0, 3)
                        val laX = values[0]; val laY = values[1]; val laZ = values[2]

                        // Prepare orientation(s)
                        var haveGM = false
                        var yawGMdeg = Double.NaN
                        var oriAgeGM = Double.NaN
                        if (hasGravity && hasMagnetic) {
                            // Build GM rotation
                            SensorManager.getRotationMatrix(rotMatrixGM, null, gravity, magnetic)
                            SensorManager.getOrientation(rotMatrixGM, orientationValsGM)
                            yawGMdeg = Math.toDegrees(orientationValsGM[0].toDouble())
                            val oriTs = maxOf(latestGRAV.ts, latestMAG.ts)
                            oriAgeGM = ((curr - oriTs).coerceAtLeast(0L)) * 1e-9
                            haveGM = true
                        }

                        var haveRV = false
                        var yawRVdeg = Double.NaN
                        var oriAgeRV = Double.NaN
                        if (hasRV) {
                            // Build RV rotation
                            SensorManager.getRotationMatrixFromVector(rotMatrixRV, rotVec)
                            SensorManager.getOrientation(rotMatrixRV, orientationValsRV)
                            yawRVdeg = Math.toDegrees(orientationValsRV[0].toDouble())
                            oriAgeRV = ((curr - latestRotTsNs).coerceAtLeast(0L)) * 1e-9
                            haveRV = true
                        }

                        // ----- Compute P along UI heading using different pipelines -----
                        // (World frame: X=East, Y=North)
                        latestP_gm = 0.0
                        latestP_rv = 0.0
                        latestP_accGM = 0.0

                        if (haveGM) {
                            val accWorldX_gm = rotMatrixGM[0]*laX + rotMatrixGM[1]*laY + rotMatrixGM[2]*laZ // East
                            val accWorldY_gm = rotMatrixGM[3]*laX + rotMatrixGM[4]*laY + rotMatrixGM[5]*laZ // North
                            latestP_gm = accWorldX_gm * dirFx + accWorldY_gm * dirFy
                        }

                        if (haveRV) {
                            val accWorldX_rv = rotMatrixRV[0]*laX + rotMatrixRV[1]*laY + rotMatrixRV[2]*laZ // East
                            val accWorldY_rv = rotMatrixRV[3]*laX + rotMatrixRV[4]*laY + rotMatrixRV[5]*laZ // North
                            latestP_rv = accWorldX_rv * dirFx + accWorldY_rv * dirFy
                        }

                        // ACC-based recomputation (bypasses vendor LA filtering)
                        var accAgeSec = Double.NaN
                        if (haveGM && latestACC.ts != 0L) {
                            val ax = latestACC.v[0]; val ay = latestACC.v[1]; val az = latestACC.v[2]
                            accAgeSec = ((curr - latestACC.ts).coerceAtLeast(0L)) * 1e-9
                            val aWorldX = rotMatrixGM[0]*ax + rotMatrixGM[1]*ay + rotMatrixGM[2]*az
                            val aWorldY = rotMatrixGM[3]*ax + rotMatrixGM[4]*ay + rotMatrixGM[5]*az
                            val aWorldZ = rotMatrixGM[6]*ax + rotMatrixGM[7]*ay + rotMatrixGM[8]*az
                            // Remove gravity in world frame
                            val linWorldX = aWorldX
                            val linWorldY = aWorldY
                            val linWorldZ = aWorldZ - SensorManager.STANDARD_GRAVITY
                            // Project onto heading (XY only)
                            latestP_accGM = linWorldX * dirFx + linWorldY * dirFy
                            // (linWorldZ not used for P; logged path is horizontal P)
                        }

                        // ===== Integrate to V =====
                        if (prevForLine != 0L) {
                            if (haveGM) vAccum_gm += dtSec * latestP_gm
                            if (haveRV) vAccum_rv += dtSec * latestP_rv
                            if (!latestP_accGM.isNaN()) vAccum_accGM += dtSec * latestP_accGM
                        }

                        // ---- LA CSV line ----
                        runCatching {
                            val w = getWriter(sensorType) ?: return@runCatching
                            val line = buildString {
                                // Core timing & LA
                                append(prevForLine); append(",")
                                append(curr); append(",")
                                append(String.format(Locale.US, "%.6f", laClockSec)); append(",")
                                append(String.format(Locale.US, "%.6f", laX)); append(",")
                                append(String.format(Locale.US, "%.6f", laY)); append(",")
                                append(String.format(Locale.US, "%.6f", laZ)); append(",")

                                // Primary P,V (GM) to keep compatibility with earlier analyses
                                if (haveGM) append(String.format(Locale.US, "%.5f", latestP_gm)) else append("")
                                append(",")
                                if (haveGM) append(String.format(Locale.US, "%.6f", vAccum_gm)) else append("")
                                // === Diagnostics ===
                                append(",")
                                // oriAgeGM, yawGMdeg
                                if (haveGM) append(String.format(Locale.US, "%.4f", oriAgeGM)) else append("")
                                append(",")
                                if (haveGM) append(String.format(Locale.US, "%.1f", yawGMdeg)) else append("")

                                append(",")
                                // P_rv, V_rv
                                if (haveRV) append(String.format(Locale.US, "%.5f", latestP_rv)) else append("")
                                append(",")
                                if (haveRV) append(String.format(Locale.US, "%.6f", vAccum_rv)) else append("")

                                append(",")
                                // oriAgeRV, yawRVdeg
                                if (haveRV) append(String.format(Locale.US, "%.4f", oriAgeRV)) else append("")
                                append(",")
                                if (haveRV) append(String.format(Locale.US, "%.1f", yawRVdeg)) else append("")

                                append(",")
                                // P_accGM, V_accGM, accAgeSec
                                if (!latestP_accGM.isNaN()) append(String.format(Locale.US, "%.5f", latestP_accGM)) else append("")
                                append(",")
                                if (!vAccum_accGM.isNaN()) append(String.format(Locale.US, "%.6f", vAccum_accGM)) else append("")
                                append(",")
                                if (!accAgeSec.isNaN()) append(String.format(Locale.US, "%.4f", accAgeSec)) else append("")

                                append("\n")
                            }
                            w.write(line); w.flush()
                        }.onFailure {
                            if (isLogging) android.util.Log.e("SensorLogger", "LA write failed", it)
                        }
                    }

                    Sensor.TYPE_ACCELEROMETER -> {
                        latestACC.ts = sensorTsNs
                        System.arraycopy(values, 0, latestACC.v, 0, 3)
                        writeSimple(sensorType, sensorTsNs, values)
                    }
                    Sensor.TYPE_GRAVITY -> {
                        // (already copied above)
                        writeSimple(sensorType, sensorTsNs, values)
                    }
                    Sensor.TYPE_MAGNETIC_FIELD -> {
                        // (already copied above)
                        writeSimple(sensorType, sensorTsNs, values)
                    }
                    Sensor.TYPE_ROTATION_VECTOR -> {
                        // Also log the raw rotation vector for reference (optional)
                        writeSimple(sensorType, sensorTsNs, values)
                    }
                }

                if (!isLogging || !writersOpen) return@synchronized

                // ---- Combined snapshot CSV remains as before ----
                runCatching {
                    val cw = getCombinedWriter() ?: return@runCatching
                    val line = buildString {
                        append(sysTimestamp)

                        fun addSnapshot(s: Snapshot) {
                            append(",")
                            if (s.ts == 0L) append("") else append(s.ts)
                            for (i in 0..2) {
                                append(",")
                                val v = s.v[i]
                                if (v.isNaN()) append("") else append(String.format(Locale.US, "%.6f", v))
                            }
                        }

                        addSnapshot(latestLA)
                        addSnapshot(latestACC)
                        addSnapshot(latestGRAV)
                        addSnapshot(latestMAG)

                        append(",")
                        // Keep P column as GM P if available; else 0
                        val pForCombined = if (hasGravity && hasMagnetic) latestP_gm else 0.0
                        append(String.format(Locale.US, "%.5f", pForCombined))
                        append("\n")
                    }
                    cw.write(line)
                    cw.flush()
                }.onFailure {
                    if (isLogging) android.util.Log.e("SensorLogger", "combined write failed", it)
                }
            }
        }
    }

    private fun writeSimple(sensorType: Int, ts: Long, values: FloatArray) {
        runCatching {
            val w = getWriter(sensorType) ?: return@runCatching
            val line = buildString {
                append(ts)
                values.forEach { append(","); append(it) }
                append("\n")
            }
            w.write(line); w.flush()
        }.onFailure {
            if (isLogging) android.util.Log.e("SensorLogger", "per-sensor write failed", it)
        }
    }

    override fun onAccuracyChanged(sensor: Sensor?, accuracy: Int) = Unit

    private fun getWriter(sensorType: Int): FileWriter? {
        if (!writersOpen) return null
        return fileWriters.getOrPut(sensorType) {
            val typeSuffix = when (sensorType) {
                Sensor.TYPE_LINEAR_ACCELERATION -> "la"
                Sensor.TYPE_ACCELEROMETER      -> "acc"
                Sensor.TYPE_GRAVITY            -> "grav"
                Sensor.TYPE_MAGNETIC_FIELD     -> "mag"
                Sensor.TYPE_ROTATION_VECTOR    -> "rotvec"
                else                           -> "other"
            }

            val fileName = "run_${sessionTimestamp}_$typeSuffix.csv"
            val file = File(logDir, fileName)

            val writer = FileWriter(file, true)
            if (file.length() == 0L) {
                val header = when (sensorType) {
                    // LA CSV: primary (GM) + diagnostics
                    // prevLaTsNs,laTsNs,laClockSec,laX,laY,laZ,P,V,oriAgeGM,yawGMdeg,P_rv,V_rv,oriAgeRV,yawRVdeg,P_accGM,V_accGM,accAgeSec
                    Sensor.TYPE_LINEAR_ACCELERATION ->
                        "prevLaTsNs,laTsNs,laClockSec,laX,laY,laZ,P,V,oriAgeGM,yawGMdeg,P_rv,V_rv,oriAgeRV,yawRVdeg,P_accGM,V_accGM,accAgeSec\n"
                    else ->
                        "timestamp,val0,val1,val2\n"
                }
                writer.write(header)
                writer.flush()
            }
            writer
        }
    }

    private fun getCombinedWriter(): FileWriter? {
        if (!writersOpen) return null
        if (combinedWriter == null) {
            val file = File(logDir, "run_${sessionTimestamp}_combined.csv")
            combinedWriter = FileWriter(file, true)
            if (file.length() == 0L) {
                val header = "sysTs," +
                        "laTs,laX,laY,laZ," +
                        "accTs,accX,accY,accZ," +
                        "gravTs,gravX,gravY,gravZ," +
                        "magTs,magX,magY,magZ," +
                        "P\n"
                combinedWriter!!.write(header)
                combinedWriter!!.flush()
            }
        }
        return combinedWriter
    }

    private fun closeWriters() {
        fun closeQuietly(w: Writer?) = runCatching { w?.flush(); w?.close() }

        try {
            fileWriters.values.forEach { closeQuietly(it) }
            fileWriters.clear()
        } finally {
            closeQuietly(combinedWriter)
            combinedWriter = null
        }
    }

    companion object {
        const val EXTRA_AZIMUTH_DEG = "extra_azimuth_deg"
    }
}
