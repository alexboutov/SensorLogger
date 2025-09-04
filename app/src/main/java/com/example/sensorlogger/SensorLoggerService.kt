
package com.example.sensorlogger

import android.app.*
import android.content.Context
import android.content.Intent
import android.hardware.*
import android.os.*
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
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

    private val gravity = FloatArray(3)
    private val magnetic = FloatArray(3)
    private var hasGravity = false
    private var hasMagnetic = false

    private val rotationMatrix = FloatArray(9)

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private lateinit var wakeLock: PowerManager.WakeLock

    private val POC = 90f

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
        val timestamp = System.currentTimeMillis()
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
            val writer = getWriter(sensorType)
            val line = buildString {
                append(timestamp)
                values.forEach { append(",$it") }

                if (sensorType == Sensor.TYPE_LINEAR_ACCELERATION && hasGravity && hasMagnetic) {
                    SensorManager.getRotationMatrix(rotationMatrix, null, gravity, magnetic)
                    val accDevice = values
                    val accWorldX = rotationMatrix[0] * accDevice[0] + rotationMatrix[1] * accDevice[1] + rotationMatrix[2] * accDevice[2]
                    val accWorldY = rotationMatrix[3] * accDevice[0] + rotationMatrix[4] * accDevice[1] + rotationMatrix[5] * accDevice[2]
                    val flatAcc = sqrt(accWorldX * accWorldX + accWorldY * accWorldY)
                    val P = if (flatAcc != 0f) {
                        accWorldX * sin(Math.toRadians(POC.toDouble())) + accWorldY * cos(Math.toRadians(POC.toDouble()))
                    } else 0.0
                    append(",${"%.5f".format(Locale.US, P)}")
                }

                append("\n")
            }

            writer.write(line)
            writer.flush()
        }
    }

    override fun onAccuracyChanged(sensor: Sensor?, accuracy: Int) = Unit

    private fun getWriter(sensorType: Int): FileWriter {
        return fileWriters.getOrPut(sensorType) {
            val typeSuffix = when (sensorType) {
                Sensor.TYPE_LINEAR_ACCELERATION -> "la"
                Sensor.TYPE_ACCELEROMETER -> "acc"
                Sensor.TYPE_GRAVITY -> "grav"
                Sensor.TYPE_MAGNETIC_FIELD -> "mag"
                else -> "other"
            }

            val fileName = "run_${sessionTimestamp}_$typeSuffix.csv"
            val file = File(logDir, fileName)

            val writer = FileWriter(file, true)
            val header = buildString {
                append("timestamp")
                for (i in 0 until 3) append(",val$i")
                if (sensorType == Sensor.TYPE_LINEAR_ACCELERATION) append(",P")
                append("\n")
            }

            writer.write(header)
            writer
        }
    }

    private fun closeWriters() {
        fileWriters.values.forEach {
            try {
                it.flush()
                it.close()
            } catch (_: Exception) {
            }
        }
        fileWriters.clear()
    }
}
