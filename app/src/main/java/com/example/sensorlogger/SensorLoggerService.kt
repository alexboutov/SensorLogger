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
import java.text.SimpleDateFormat
import java.util.*
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

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private lateinit var wakeLock: PowerManager.WakeLock

    // “POC” angle used to project horizontal acceleration into forward axis
    private val POC = 90f
    private var latestP: Double = 0.0

    // Simple lock for consistency across concurrent writes
    private val lock = Any()

    override fun onCreate() {
        super.onCreate()
        setupForegroundNotification()
        acquireWakeLock()

        sessionTimestamp = SimpleDateFormat("yyyyMMdd_HHmmss", Locale.US).format(Date())
        logDir = File(getExternalFilesDir(null), "sensor_logs").apply { mkdirs() }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        registerSensors()
        return START_STICKY
    }

    override fun onDestroy() {
        super.onDestroy()
        unregisterSensors()
        closeWriters()
        releaseWakeLock()
        scope.cancel()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun setupForegroundNotification() {
        val channelId = "SensorLoggerChannel"
        val channel = NotificationChannel(channelId, "Sensor Logger", NotificationManager.IMPORTANCE_LOW)
        getSystemService(NotificationManager::class.java).createNotificationChannel(channel)

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
        if (wakeLock.isHeld) {
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
        val sensorType = event.sensor.type
        val sysTimestamp = System.currentTimeMillis()
        val sensorTimestamp = event.timestamp // ns since boot; we’ll store system time for CSVs, but keep this for future if needed
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

        scope.launch {
            // Update per-sensor latest snapshot and write per-sensor CSV
            synchronized(lock) {
                when (sensorType) {
                    Sensor.TYPE_LINEAR_ACCELERATION -> {
                        latestLA.ts = sysTimestamp
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
                    }
                    Sensor.TYPE_ACCELEROMETER -> {
                        latestACC.ts = sysTimestamp
                        System.arraycopy(values, 0, latestACC.v, 0, 3)
                    }
                    Sensor.TYPE_GRAVITY -> {
                        latestGRAV.ts = sysTimestamp
                        System.arraycopy(values, 0, latestGRAV.v, 0, 3)
                    }
                    Sensor.TYPE_MAGNETIC_FIELD -> {
                        latestMAG.ts = sysTimestamp
                        System.arraycopy(values, 0, latestMAG.v, 0, 3)
                    }
                }

                // Write the per-sensor CSV row (keeps your existing per-type files)
                val singleWriter = getWriter(sensorType)
                val singleLine = buildString {
                    append(sysTimestamp)
                    values.forEach { append(","); append(it) }
                    if (sensorType == Sensor.TYPE_LINEAR_ACCELERATION) {
                        append(",")
                        append(String.format(Locale.US, "%.5f", latestP))
                    }
                    append("\n")
                }
                singleWriter.write(singleLine)
                singleWriter.flush()

                // Write the combined CSV row (every sensor update triggers a snapshot line)
                val cw = getCombinedWriter()
                val line = buildString {
                    append(sysTimestamp)

                    fun addSnapshot(s: Snapshot) {
                        // 4 fields per sensor: ts, x, y, z
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
            }
        }
    }

    override fun onAccuracyChanged(sensor: Sensor?, accuracy: Int) = Unit

    private fun getWriter(sensorType: Int): FileWriter {
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
                // Header: timestamp,val0,val1,val2,(+P for LA)
                val header = buildString {
                    append("timestamp")
                    append(",val0,val1,val2")
                    if (sensorType == Sensor.TYPE_LINEAR_ACCELERATION) append(",P")
                    append("\n")
                }
                writer.write(header)
                writer.flush()
            }
            writer
        }
    }

    private fun getCombinedWriter(): FileWriter {
        if (combinedWriter == null) {
            val file = File(logDir, "run_${sessionTimestamp}_combined.csv")
            combinedWriter = FileWriter(file, true)
            if (file.length() == 0L) {
                // Header (18 columns total):
                // sysTs,
                // laTs,laX,laY,laZ,
                // accTs,accX,accY,accZ,
                // gravTs,gravX,gravY,gravZ,
                // magTs,magX,magY,magZ,
                // P
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
        return combinedWriter!!
    }

    private fun closeWriters() {
        try {
            fileWriters.values.forEach {
                try { it.flush(); it.close() } catch (_: Exception) {}
            }
            fileWriters.clear()
        } finally {
            try { combinedWriter?.flush(); combinedWriter?.close() } catch (_: Exception) {}
            combinedWriter = null
        }
    }
}
