package com.example.sensorlogger

import android.app.*
import android.content.Context
import android.content.Intent
import android.hardware.*
import android.os.*
import androidx.core.app.NotificationCompat
import kotlinx.coroutines.*
import java.io.File
import java.io.FileWriter
import java.io.Writer
import java.text.SimpleDateFormat
import java.util.*
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

    // “POC” angle used to project horizontal acceleration into forward axis
    private val POC = 90f
    private var latestP: Double = 0.0

    // LA timing (sensor-clock, in nanoseconds)
    private var prevLaTsNs: Long = 0L
    private var laClockNs: Long = 0L

    // Simple lock for consistency across concurrent writes (still useful inside single-thread)
    private val lock = Any()

    override fun onCreate() {
        super.onCreate()
        setupForegroundNotification()
        acquireWakeLock()

        sessionTimestamp = SimpleDateFormat("yyyyMMdd_HHmmss", Locale.US).format(Date())
        logDir = File(getExternalFilesDir(null), "sensor_logs").apply { mkdirs() }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        isLogging = true
        writersOpen = true
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

        val notification = NotificationCompat.Builder(this, channelId)
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
                        val dt = if (prevForLine == 0L) 0L else (curr - prevForLine).coerceAtLeast(0L)
                        laClockNs += dt
                        prevLaTsNs = curr

                        // Update latest snapshot (use sensor timestamp for LA)
                        latestLA.ts = curr
                        System.arraycopy(values, 0, latestLA.v, 0, 3)

                        // Compute/refresh P if we have orientation
                        if (hasGravity && hasMagnetic) {
                            SensorManager.getRotationMatrix(rotationMatrix, null, gravity, magnetic)
                            val accDevice = values
                            val accWorldX = rotationMatrix[0] * accDevice[0] + rotationMatrix[1] * accDevice[1] + rotationMatrix[2] * accDevice[2]
                            val accWorldY = rotationMatrix[3] * accDevice[0] + rotationMatrix[4] * accDevice[1] + rotationMatrix[5] * accDevice[2]
                            val flatAcc = sqrt(accWorldX * accWorldX + accWorldY * accWorldY)
                            latestP = if (flatAcc != 0f) {
                                accWorldX * sin(Math.toRadians(POC.toDouble())) +
                                        accWorldY * cos(Math.toRadians(POC.toDouble()))
                            } else 0.0
                        }

                        // ---- Per-sensor LA CSV (prevTs, currTs, clock, x,y,z, P) ----
                        runCatching {
                            val w = getWriter(sensorType) ?: return@runCatching
                            val line = buildString {
                                append(prevForLine); append(",")
                                append(curr); append(",")
                                append(laClockNs); append(",")
                                append(String.format(Locale.US, "%.6f", values[0])); append(",")
                                append(String.format(Locale.US, "%.6f", values[1])); append(",")
                                append(String.format(Locale.US, "%.6f", values[2])); append(",")
                                if (hasGravity && hasMagnetic) append(String.format(Locale.US, "%.5f", latestP)) else append("")
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
                        append(String.format(Locale.US, "%.5f", latestP))
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
                    // New LA schema: prevTsNs, currTsNs, accumulatedClockNs, x, y, z, P
                    Sensor.TYPE_LINEAR_ACCELERATION ->
                        "prevLaTsNs,laTsNs,laClockNs,laX,laY,laZ,P\n"
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
}
