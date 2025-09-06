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

    // For rotation & P computation
    private val gravity = FloatArray(3)
    private val magnetic = FloatArray(3)
    private var hasGravity = false
    private var hasMagnetic = false
    private val rotationMatrix = FloatArray(9)
    private val orientationVals = FloatArray(3) // [azimuth(Z), pitch(X), roll(Y)]

    // Projection direction from UI azimuth (deg clockwise from North).
    // World frame from getRotationMatrix: X=East, Y=North, Z=Up.
    private var azimuthDeg: Double = 0.0
    private var dirFx: Double = 0.0   // East component = sin(az)
    private var dirFy: Double = 1.0   // North component = cos(az)
    private var latestP: Double = 0.0 // m/s^2 along heading

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

    // ===== Velocity accumulator (per spec) =====
    private var vAccum: Double = 0.0  // V[i]

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
        // START: open run and reset accumulators
        isLogging = true
        writersOpen = true

        // 1) Read azimuth from UI and precompute direction vector (X=East, Y=North)
        azimuthDeg = intent?.getDoubleExtra(EXTRA_AZIMUTH_DEG, 0.0) ?: 0.0
        val azRad = Math.toRadians(azimuthDeg)
        dirFx = sin(azRad) // along East
        dirFy = cos(azRad) // along North

        // 2) Reset integrators/clocks for a clean session
        vAccum = 0.0
        prevLaTsNs = 0L
        laClockSec = 0.0

        registerSensors()
        return START_STICKY
    }

    override fun onDestroy() {
        // STOP path: order matters
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
            Sensor.TYPE_MAGNETIC_FIELD
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
            }
            Sensor.TYPE_MAGNETIC_FIELD -> {
                System.arraycopy(values, 0, magnetic, 0, 3)
                hasMagnetic = true
            }
        }

        ioScope.launch {
            if (!isLogging || !writersOpen) return@launch

            synchronized(lock) {
                when (sensorType) {
                    Sensor.TYPE_LINEAR_ACCELERATION -> {
                        // ---- Update LA timing with sensor clock ----
                        val prevForLine = prevLaTsNs
                        val curr = sensorTsNs
                        val dtNs = if (prevForLine == 0L) 0L else (curr - prevForLine).coerceAtLeast(0L)
                        val dtSec = if (prevForLine == 0L) 0.0 else dtNs.toDouble() * 1e-9
                        laClockSec += dtSec
                        prevLaTsNs = curr

                        // Update latest snapshot (use sensor timestamp for LA)
                        latestLA.ts = curr
                        System.arraycopy(values, 0, latestLA.v, 0, 3)

                        // Compute/refresh P (projection onto heading) if we have orientation
                        var oriAgeSecStr = ""
                        var yawDegUsedStr = ""
                        if (hasGravity && hasMagnetic) {
                            SensorManager.getRotationMatrix(rotationMatrix, null, gravity, magnetic)

                            // World-frame acceleration (X=East, Y=North)
                            val accDevice = values
                            val accWorldX = rotationMatrix[0] * accDevice[0] + rotationMatrix[1] * accDevice[1] + rotationMatrix[2] * accDevice[2] // East
                            val accWorldY = rotationMatrix[3] * accDevice[0] + rotationMatrix[4] * accDevice[1] + rotationMatrix[5] * accDevice[2] // North
                            val flatAcc = sqrt(accWorldX * accWorldX + accWorldY * accWorldY)

                            latestP = if (flatAcc != 0f) {
                                accWorldX * dirFx + accWorldY * dirFy
                            } else 0.0

                            // --- Instrumentation: orientation age + yaw used ---
                            val oriTsNs = maxOf(latestGRAV.ts, latestMAG.ts)
                            val oriAgeSec = ((sensorTsNs - oriTsNs).coerceAtLeast(0L)) * 1e-9
                            SensorManager.getOrientation(rotationMatrix, orientationVals)
                            val yawDegUsed = Math.toDegrees(orientationVals[0].toDouble())

                            oriAgeSecStr = String.format(Locale.US, "%.4f", oriAgeSec)
                            yawDegUsedStr = String.format(Locale.US, "%.1f", yawDegUsed)
                        } else {
                            // Orientation not ready → keep P at 0.0 for V accumulation; leave oriAgeSec/yaw blank
                            latestP = 0.0
                        }

                        // ===== V accumulation in SECONDS =====
                        if (prevForLine != 0L) {
                            vAccum += dtSec * latestP
                        }

                        // ---- Per-sensor LA CSV (prevTs, currTs, clockSec, x,y,z, P, V, oriAgeSec, yawDegUsed) ----
                        runCatching {
                            val w = getWriter(sensorType) ?: return@runCatching
                            val line = buildString {
                                append(prevForLine); append(",")
                                append(curr); append(",")
                                append(String.format(Locale.US, "%.6f", laClockSec)); append(",")
                                append(String.format(Locale.US, "%.6f", values[0])); append(",")
                                append(String.format(Locale.US, "%.6f", values[1])); append(",")
                                append(String.format(Locale.US, "%.6f", values[2])); append(",")
                                // P
                                if (hasGravity && hasMagnetic) {
                                    append(String.format(Locale.US, "%.5f", latestP))
                                } else {
                                    append("")
                                }
                                append(",")
                                // V
                                append(String.format(Locale.US, "%.6f", vAccum))
                                // Instrumentation columns
                                append(","); append(oriAgeSecStr)
                                append(","); append(yawDegUsedStr)
                                append("\n")
                            }
                            w.write(line); w.flush()
                        }.onFailure {
                            if (isLogging) android.util.Log.e("SensorLogger", "LA write failed", it)
                        }
                    }

                    Sensor.TYPE_ACCELEROMETER -> {
                        latestACC.ts = sensorTsNs // use sensor ts for ACC too (more consistent)
                        System.arraycopy(values, 0, latestACC.v, 0, 3)
                    }
                    Sensor.TYPE_GRAVITY -> {
                        latestGRAV.ts = sensorTsNs
                        System.arraycopy(values, 0, latestGRAV.v, 0, 3)
                    }
                    Sensor.TYPE_MAGNETIC_FIELD -> {
                        latestMAG.ts = sensorTsNs
                        System.arraycopy(values, 0, latestMAG.v, 0, 3)
                    }
                }

                if (!isLogging || !writersOpen) return@synchronized

                // ---- Per-sensor CSV for non-LA sensors (timestamp,val0,val1,val2) ----
                if (sensorType != Sensor.TYPE_LINEAR_ACCELERATION) {
                    runCatching {
                        val singleWriter = getWriter(sensorType) ?: return@runCatching
                        val line = buildString {
                            append(sensorTsNs)
                            values.forEach { append(","); append(it) }
                            append("\n")
                        }
                        singleWriter.write(line)
                        singleWriter.flush()
                    }.onFailure {
                        if (isLogging) android.util.Log.e("SensorLogger", "per-sensor write failed", it)
                    }
                }

                // ---- Combined snapshot CSV (sysTs, laTsNs..., accTsNs..., gravTsNs..., magTsNs..., P) ----
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
                        append(String.format(Locale.US, "%.5f", if (hasGravity && hasMagnetic) latestP else 0.0))
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

    override fun onAccuracyChanged(sensor: Sensor?, accuracy: Int) = Unit

    private fun getWriter(sensorType: Int): FileWriter? {
        if (!writersOpen) return null
        return fileWriters.getOrPut(sensorType) {
            val typeSuffix = when (sensorType) {
                Sensor.TYPE_LINEAR_ACCELERATION -> "la"
                Sensor.TYPE_ACCELEROMETER      -> "acc"
                Sensor.TYPE_GRAVITY            -> "grav"
                Sensor.TYPE_MAGNETIC_FIELD     -> "mag"
                else                           -> "other"
            }

            val fileName = "run_${sessionTimestamp}_$typeSuffix.csv"
            val file = File(logDir, fileName)

            val writer = FileWriter(file, true)
            if (file.length() == 0L) {
                val header = when (sensorType) {
                    // Updated LA schema with instrumentation:
                    // prevLaTsNs, laTsNs, laClockSec, laX, laY, laZ, P, V, oriAgeSec, yawDegUsed
                    Sensor.TYPE_LINEAR_ACCELERATION ->
                        "prevLaTsNs,laTsNs,laClockSec,laX,laY,laZ,P,V,oriAgeSec,yawDegUsed\n"
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
