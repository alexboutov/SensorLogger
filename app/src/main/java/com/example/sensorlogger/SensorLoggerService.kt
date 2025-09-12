package com.example.sensorlogger

// ==============================
// Imports
// ==============================
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
import kotlin.math.abs
import kotlin.math.cos
import kotlin.math.sin
import kotlin.math.sqrt

class SensorLoggerService : Service(), SensorEventListener {

    // ==============================
    // Constants (class scope)
    // ==============================
    private val ORI_STALE_SEC = 0.040            // 40 ms
    private val ACC_STALE_SEC = 0.040            // 40 ms
    private val LA_MAG_SAT = 25.0                // m/s^2 (LA magnitude too large)
    private val LA_JUMP_THRESH = 5.0             // m/s^2 (per-sample jump magnitude)
    private val P_OUTLIER_THRESH = 10.0          // m/s^2 along heading (implausible for human-scale motion)
    private val GRAV_NORM = SensorManager.STANDARD_GRAVITY.toDouble() // ~9.80665
    private val GRAV_TOL = 1.5                   // m/s^2 tolerance
    private val MAG_MIN_UT = 20.0                // microTesla lower bound
    private val MAG_MAX_UT = 70.0                // microTesla upper bound

    // Smoothing window
    private val N_SMOOTH = 20
    private val WINDOW_SIZE = 2 * N_SMOOTH + 1

    // Post-processing config
    private val DIST_METERS = 20.0               // assumed known distance per test

    // ==============================
    // Types (class scope)
    // ==============================
    private data class Snapshot(
        var ts: Long = 0L,
        val v: FloatArray = floatArrayOf(Float.NaN, Float.NaN, Float.NaN)
    )

    private data class LaRow(
        val prevLaTsNs: Long,
        val laTsNs: Long,
        val laClockSec: Double,
        val laX: Float, val laY: Float, val laZ: Float,
        val azimuthDeg: Double,
        // Raw P/V
        val haveGM: Boolean,
        val p_gm: Double,
        val v_gm: Double,
        val oriAgeGM: Double?,
        val yawGMdeg: Double?,
        val haveRV: Boolean,
        val p_rv: Double,
        val v_rv: Double,
        val oriAgeRV: Double?,
        val yawRVdeg: Double?,
        val p_accGM: Double?,
        val v_accGM: Double?,
        val accAgeSec: Double?,

        // Smoothed P (centered SMA)
        var p_gm_smooth: Double? = null,
        var p_rv_smooth: Double? = null,
        var p_accGM_smooth: Double? = null,

        // Smoothed velocity and distance (displacement along heading)
        var v_gm_smooth: Double? = null,
        var s_gm_smooth: Double? = null,
        var v_rv_smooth: Double? = null,
        var s_rv_smooth: Double? = null,
        var v_accGM_smooth: Double? = null,
        var s_accGM_smooth: Double? = null,

        // Flags
        val flagOriStaleGM: Boolean?,
        val flagOriStaleRV: Boolean?,
        val flagAccStale: Boolean?,
        val flagLaSat: Boolean,
        val flagLaJump: Boolean,
        val flagPgmOut: Boolean?,
        val flagPrvOut: Boolean?,
        val flagGravAnom: Boolean?,
        val flagMagAnom: Boolean?
    )

    // ==============================
    // Variables (class scope)
    // ==============================
    private val sensorManager by lazy { getSystemService(Context.SENSOR_SERVICE) as SensorManager }

    private lateinit var logDir: File
    private lateinit var sessionTimestamp: String

    private val fileWriters = mutableMapOf<Int, FileWriter>()
    private var combinedWriter: FileWriter? = null

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
    private var rotVec: FloatArray = FloatArray(5)

    // Projection direction from UI azimuth (deg clockwise from North).
    // World frame: X=East, Y=North, Z=Up.
    private var azimuthDeg: Double = 0.0
    private var dirFx: Double = 0.0   // East component = sin(az)
    private var dirFy: Double = 1.0   // North component = cos(az)

    // P components and accumulators (raw)
    private var latestP_gm: Double = 0.0
    private var latestP_rv: Double = 0.0
    private var latestP_accGM: Double = 0.0

    private var vAccum_gm: Double = 0.0     // integrates raw P_gm
    private var vAccum_rv: Double = 0.0     // integrates raw P_rv
    private var vAccum_accGM: Double = 0.0  // integrates raw P_accGM

    /** Concurrency & lifecycle guards */
    private val ioErrorHandler = CoroutineExceptionHandler { _, e ->
        android.util.Log.d("SensorLogger", "I/O after stop (ignored): ${e.message}")
    }
    private val serviceJob = SupervisorJob()
    private val writeExecutor = Executors.newSingleThreadExecutor()
    private val ioScope = CoroutineScope(writeExecutor.asCoroutineDispatcher() + serviceJob + ioErrorHandler)

    @Volatile private var isLogging = false
    @Volatile private var writersOpen = false

    private lateinit var wakeLock: PowerManager.WakeLock

    // LA timing
    private var prevLaTsNs: Long = 0L
    private var laClockSec: Double = 0.0

    // For LA jump/jerk diagnostics
    private val prevLaVec = FloatArray(3) { 0f }
    private var hasPrevLa = false

    // Simple lock
    private val lock = Any()

    // Smoothing buffers and aligned row buffers per pipe
    private val bufPgmVals = ArrayDeque<Double>()
    private val bufPgmRows = ArrayDeque<LaRow>()
    private val bufPrvVals = ArrayDeque<Double>()
    private val bufPrvRows = ArrayDeque<LaRow>()
    private val bufPaccVals = ArrayDeque<Double>()
    private val bufPaccRows = ArrayDeque<LaRow>()

    // Smoothed integration accumulators per pipe
    private var vSmAccum_gm = 0.0
    private var vSmAccum_rv = 0.0
    private var vSmAccum_accGM = 0.0
    private var sSmAccum_gm = 0.0
    private var sSmAccum_rv = 0.0
    private var sSmAccum_accGM = 0.0
    private var lastSmaTsNs_gm = 0L
    private var lastSmaTsNs_rv = 0L
    private var lastSmaTsNs_accGM = 0L

    private val pendingRows: ArrayDeque<LaRow> = ArrayDeque()

    // ==============================
    // Lifecycle
    // ==============================
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

        azimuthDeg = when {
            intent?.hasExtra(EXTRA_AZIMUTH_DEG) == true ->
                intent.getDoubleExtra(EXTRA_AZIMUTH_DEG, 0.0)
            intent?.hasExtra("com.example.sensorlogger.EXTRA_AZIMUTH_DEG") == true ->
                intent.getDoubleExtra("com.example.sensorlogger.EXTRA_AZIMUTH_DEG", 0.0)
            else -> 0.0
        }
        val azRad = Math.toRadians(azimuthDeg)
        dirFx = sin(azRad) // East
        dirFy = cos(azRad) // North
        android.util.Log.d("SensorLogger", "UI azimuthDeg=$azimuthDeg°  dirFx=$dirFx dirFy=$dirFy")

        vAccum_gm = 0.0
        vAccum_rv = 0.0
        vAccum_accGM = 0.0
        prevLaTsNs = 0L
        laClockSec = 0.0

        hasPrevLa = false
        prevLaVec[0] = 0f; prevLaVec[1] = 0f; prevLaVec[2] = 0f

        bufPgmVals.clear(); bufPgmRows.clear()
        bufPrvVals.clear(); bufPrvRows.clear()
        bufPaccVals.clear(); bufPaccRows.clear()
        vSmAccum_gm = 0.0; vSmAccum_rv = 0.0; vSmAccum_accGM = 0.0
        sSmAccum_gm = 0.0; sSmAccum_rv = 0.0; sSmAccum_accGM = 0.0
        lastSmaTsNs_gm = 0L; lastSmaTsNs_rv = 0L; lastSmaTsNs_accGM = 0L
        pendingRows.clear()

        registerSensors()
        return START_STICKY
    }

    override fun onDestroy() {
        isLogging = false
        unregisterSensors()

        // Flush pending rows
        synchronized(lock) {
            while (pendingRows.isNotEmpty()) {
                writeLaRow(pendingRows.removeFirst())
            }
        }

        serviceJob.cancelChildren()
        closeWriters() // close files so we can post-process safely

        // === Post-process: detrend velocities and rewrite LA CSV with new columns + footer ===
        runCatching { postProcessLaCsv(DIST_METERS) }
            .onFailure { android.util.Log.e("SensorLogger", "post-process failed", it) }

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

    // ==============================
    // Sensor registration
    // ==============================
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

    // ==============================
    // onSensorChanged processing
    // ==============================
    override fun onSensorChanged(event: SensorEvent) {
        if (!isLogging) return

        val sensorType = event.sensor.type
        val sysTimestamp = System.currentTimeMillis()
        val sensorTsNs = event.timestamp
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
                    rotVec = FloatArray(maxOf(5, values.size))
                }
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

                        // ===== Magnitudes / norms for diagnostics =====
                        val laMag = sqrt((laX * laX + laY * laY + laZ * laZ).toDouble())
                        val gravMag = if (hasGravity) sqrt(
                            (gravity[0] * gravity[0] + gravity[1] * gravity[1] + gravity[2] * gravity[2]).toDouble()
                        ) else Double.NaN
                        val magMagUt = if (hasMagnetic) sqrt(
                            (magnetic[0] * magnetic[0] + magnetic[1] * magnetic[1] + magnetic[2] * magnetic[2]).toDouble()
                        ) else Double.NaN

                        // LA jump (jerk-like) per-sample
                        val laJumpMag = if (hasPrevLa) {
                            val dx = laX - prevLaVec[0]
                            val dy = laY - prevLaVec[1]
                            val dz = laZ - prevLaVec[2]
                            sqrt((dx * dx + dy * dy + dz * dz).toDouble())
                        } else 0.0
                        prevLaVec[0] = laX; prevLaVec[1] = laY; prevLaVec[2] = laZ
                        hasPrevLa = true

                        // Prepare orientation(s)
                        var haveGM = false
                        var yawGMdeg = Double.NaN
                        var oriAgeGM = Double.NaN
                        if (hasGravity && hasMagnetic) {
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
                            SensorManager.getRotationMatrixFromVector(rotMatrixRV, rotVec)
                            SensorManager.getOrientation(rotMatrixRV, orientationValsRV)
                            yawRVdeg = Math.toDegrees(orientationValsRV[0].toDouble())
                            oriAgeRV = ((curr - latestRotTsNs).coerceAtLeast(0L)) * 1e-9
                            haveRV = true
                        }

                        // ----- Compute P along UI heading (ENU) -----
                        latestP_gm = 0.0
                        latestP_rv = 0.0
                        latestP_accGM = 0.0

                        if (haveGM) {
                            // GM -> world (use R^T)
                            val accWorldX_gm = rotMatrixGM[0]*laX + rotMatrixGM[3]*laY + rotMatrixGM[6]*laZ
                            val accWorldY_gm = rotMatrixGM[1]*laX + rotMatrixGM[4]*laY + rotMatrixGM[7]*laZ
                            latestP_gm = accWorldX_gm * dirFx + accWorldY_gm * dirFy
                        }

                        if (haveRV) {
                            // RV -> world (use R^T)
                            val accWorldX_rv = rotMatrixRV[0]*laX + rotMatrixRV[3]*laY + rotMatrixRV[6]*laZ
                            val accWorldY_rv = rotMatrixRV[1]*laX + rotMatrixRV[4]*laY + rotMatrixRV[7]*laZ
                            latestP_rv = accWorldX_rv * dirFx + accWorldY_rv * dirFy
                        }

                        // ACC-based recomputation: (ACC - GRAV) in device frame, then rotate with R^T
                        var accAgeSec = Double.NaN
                        if (haveGM && latestACC.ts != 0L) {
                            val ax = latestACC.v[0]; val ay = latestACC.v[1]; val az = latestACC.v[2]
                            accAgeSec = ((curr - latestACC.ts).coerceAtLeast(0L)) * 1e-9

                            val linDevX = ax - gravity[0]
                            val linDevY = ay - gravity[1]
                            val linDevZ = az - gravity[2]

                            val linWorldX = rotMatrixGM[0]*linDevX + rotMatrixGM[3]*linDevY + rotMatrixGM[6]*linDevZ // East
                            val linWorldY = rotMatrixGM[1]*linDevX + rotMatrixGM[4]*linDevY + rotMatrixGM[7]*linDevZ // North
                            latestP_accGM = linWorldX * dirFx + linWorldY * dirFy
                        }

                        // ===== Integrate to V (RAW P ONLY) =====
                        if (prevForLine != 0L) {
                            if (haveGM) vAccum_gm += dtSec * latestP_gm
                            if (haveRV) vAccum_rv += dtSec * latestP_rv
                            if (!latestP_accGM.isNaN()) vAccum_accGM += dtSec * latestP_accGM
                        }

                        // ======== Flags ========
                        val flagOriStaleGM = if (haveGM && !oriAgeGM.isNaN()) (oriAgeGM > ORI_STALE_SEC) else null
                        val flagOriStaleRV = if (haveRV && !oriAgeRV.isNaN()) (oriAgeRV > ORI_STALE_SEC) else null
                        val flagAccStale   = if (!accAgeSec.isNaN()) (accAgeSec > ACC_STALE_SEC) else null

                        val flagLaSat  = laMag > LA_MAG_SAT
                        val flagLaJump = hasPrevLa && laJumpMag > LA_JUMP_THRESH

                        val flagPgmOut = if (haveGM) (abs(latestP_gm) > P_OUTLIER_THRESH) else null
                        val flagPrvOut = if (haveRV) (abs(latestP_rv) > P_OUTLIER_THRESH) else null

                        val flagGravAnom = if (hasGravity) (abs(gravMag - GRAV_NORM) > GRAV_TOL) else null
                        val flagMagAnom  = if (hasMagnetic) (magMagUt < MAG_MIN_UT || magMagUt > MAG_MAX_UT) else null

                        // ======== Build row ========
                        val row = LaRow(
                            prevLaTsNs = prevForLine,
                            laTsNs = curr,
                            laClockSec = laClockSec,
                            laX = laX, laY = laY, laZ = laZ,
                            azimuthDeg = azimuthDeg,
                            haveGM = haveGM,
                            p_gm = latestP_gm,
                            v_gm = vAccum_gm,
                            oriAgeGM = if (haveGM) oriAgeGM else null,
                            yawGMdeg = if (haveGM) yawGMdeg else null,
                            haveRV = haveRV,
                            p_rv = latestP_rv,
                            v_rv = vAccum_rv,
                            oriAgeRV = if (haveRV) oriAgeRV else null,
                            yawRVdeg = if (haveRV) yawRVdeg else null,
                            p_accGM = if (!latestP_accGM.isNaN()) latestP_accGM else null,
                            v_accGM = if (!vAccum_accGM.isNaN()) vAccum_accGM else null,
                            accAgeSec = if (!accAgeSec.isNaN()) accAgeSec else null,
                            // smoothed (filled later)
                            p_gm_smooth = null, p_rv_smooth = null, p_accGM_smooth = null,
                            v_gm_smooth = null, s_gm_smooth = null,
                            v_rv_smooth = null, s_rv_smooth = null,
                            v_accGM_smooth = null, s_accGM_smooth = null,
                            // flags
                            flagOriStaleGM = flagOriStaleGM,
                            flagOriStaleRV = flagOriStaleRV,
                            flagAccStale = flagAccStale,
                            flagLaSat = flagLaSat,
                            flagLaJump = flagLaJump,
                            flagPgmOut = flagPgmOut,
                            flagPrvOut = flagPrvOut,
                            flagGravAnom = flagGravAnom,
                            flagMagAnom = flagMagAnom
                        )
                        pendingRows.addLast(row)

                        // ======== Update P buffers (centered SMA) ========
                        if (haveGM) {
                            bufPgmVals.addLast(latestP_gm); bufPgmRows.addLast(row)
                            if (bufPgmVals.size > WINDOW_SIZE) { bufPgmVals.removeFirst(); bufPgmRows.removeFirst() }
                        }
                        if (haveRV) {
                            bufPrvVals.addLast(latestP_rv); bufPrvRows.addLast(row)
                            if (bufPrvVals.size > WINDOW_SIZE) { bufPrvVals.removeFirst(); bufPrvRows.removeFirst() }
                        }
                        if (row.p_accGM != null) {
                            bufPaccVals.addLast(latestP_accGM); bufPaccRows.addLast(row)
                            if (bufPaccVals.size > WINDOW_SIZE) { bufPaccVals.removeFirst(); bufPaccRows.removeFirst() }
                        }

                        // ======== Fill centered SMA & integrate smoothed ========
                        if (bufPgmVals.size == WINDOW_SIZE) {
                            val centerRow = dequeNth(bufPgmRows, N_SMOOTH)
                            if (centerRow != null) {
                                val psm = averageDeque(bufPgmVals)
                                centerRow.p_gm_smooth = psm
                                val ts = centerRow.laTsNs
                                if (lastSmaTsNs_gm != 0L) {
                                    val dt = ((ts - lastSmaTsNs_gm).coerceAtLeast(0L)).toDouble() * 1e-9
                                    vSmAccum_gm += dt * psm
                                    sSmAccum_gm += dt * vSmAccum_gm
                                }
                                lastSmaTsNs_gm = ts
                                centerRow.v_gm_smooth = vSmAccum_gm
                                centerRow.s_gm_smooth = sSmAccum_gm
                            }
                        }
                        if (bufPrvVals.size == WINDOW_SIZE) {
                            val centerRow = dequeNth(bufPrvRows, N_SMOOTH)
                            if (centerRow != null) {
                                val psm = averageDeque(bufPrvVals)
                                centerRow.p_rv_smooth = psm
                                val ts = centerRow.laTsNs
                                if (lastSmaTsNs_rv != 0L) {
                                    val dt = ((ts - lastSmaTsNs_rv).coerceAtLeast(0L)).toDouble() * 1e-9
                                    vSmAccum_rv += dt * psm
                                    sSmAccum_rv += dt * vSmAccum_rv
                                }
                                lastSmaTsNs_rv = ts
                                centerRow.v_rv_smooth = vSmAccum_rv
                                centerRow.s_rv_smooth = sSmAccum_rv
                            }
                        }
                        if (bufPaccVals.size == WINDOW_SIZE) {
                            val centerRow = dequeNth(bufPaccRows, N_SMOOTH)
                            if (centerRow != null) {
                                val psm = averageDeque(bufPaccVals)
                                centerRow.p_accGM_smooth = psm
                                val ts = centerRow.laTsNs
                                if (lastSmaTsNs_accGM != 0L) {
                                    val dt = ((ts - lastSmaTsNs_accGM).coerceAtLeast(0L)).toDouble() * 1e-9
                                    vSmAccum_accGM += dt * psm
                                    sSmAccum_accGM += dt * vSmAccum_accGM
                                }
                                lastSmaTsNs_accGM = ts
                                centerRow.v_accGM_smooth = vSmAccum_accGM
                                centerRow.s_accGM_smooth = sSmAccum_accGM
                            }
                        }

                        // Emit rows while we have more than N_SMOOTH pending
                        while (pendingRows.size > N_SMOOTH) {
                            writeLaRow(pendingRows.removeFirst())
                        }
                    }

                    Sensor.TYPE_ACCELEROMETER -> {
                        latestACC.ts = sensorTsNs
                        System.arraycopy(values, 0, latestACC.v, 0, 3)
                        writeSimple(sensorType, sensorTsNs, values)
                    }
                    Sensor.TYPE_GRAVITY -> {
                        writeSimple(sensorType, sensorTsNs, values)
                    }
                    Sensor.TYPE_MAGNETIC_FIELD -> {
                        writeSimple(sensorType, sensorTsNs, values)
                    }
                    Sensor.TYPE_ROTATION_VECTOR -> {
                        writeSimple(sensorType, sensorTsNs, values)
                    }
                }

                if (!isLogging || !writersOpen) return@synchronized

                // ---- Combined snapshot CSV (unchanged) ----
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

    override fun onAccuracyChanged(sensor: Sensor?, accuracy: Int) = Unit

    // ==============================
    // CSV/IO helpers & small utils
    // ==============================
    private fun <T> dequeNth(dq: ArrayDeque<T>, index: Int): T? {
        if (index < 0 || index >= dq.size) return null
        var i = 0
        for (e in dq) {
            if (i == index) return e
            i++
        }
        return null
    }

    private fun averageDeque(dq: ArrayDeque<Double>): Double {
        var sum = 0.0
        for (v in dq) sum += v
        return sum / dq.size
    }

    private fun writeLaRow(r: LaRow) {
        runCatching {
            val w = getWriter(Sensor.TYPE_LINEAR_ACCELERATION) ?: return@runCatching
            val line = buildString {
                append(r.prevLaTsNs); append(",")
                append(r.laTsNs); append(",")
                append(String.format(Locale.US, "%.6f", r.laClockSec)); append(",")
                append(String.format(Locale.US, "%.6f", r.laX)); append(",")
                append(String.format(Locale.US, "%.6f", r.laY)); append(",")
                append(String.format(Locale.US, "%.6f", r.laZ)); append(",")

                append(String.format(Locale.US, "%.3f", r.azimuthDeg)); append(",")

                if (r.haveGM) append(String.format(Locale.US, "%.5f", r.p_gm)) else append("")
                append(",")
                if (r.haveGM) append(String.format(Locale.US, "%.6f", r.v_gm)) else append("")

                append(",")
                if (r.haveGM && r.oriAgeGM != null) append(String.format(Locale.US, "%.4f", r.oriAgeGM)) else append("")
                append(",")
                if (r.haveGM && r.yawGMdeg != null) append(String.format(Locale.US, "%.1f", r.yawGMdeg)) else append("")

                append(",")
                if (r.haveRV) append(String.format(Locale.US, "%.5f", r.p_rv)) else append("")
                append(",")
                if (r.haveRV) append(String.format(Locale.US, "%.6f", r.v_rv)) else append("")

                append(",")
                if (r.haveRV && r.oriAgeRV != null) append(String.format(Locale.US, "%.4f", r.oriAgeRV)) else append("")
                append(",")
                if (r.haveRV && r.yawRVdeg != null) append(String.format(Locale.US, "%.1f", r.yawRVdeg)) else append("")

                append(",")
                if (r.p_accGM != null) append(String.format(Locale.US, "%.5f", r.p_accGM)) else append("")
                append(",")
                if (r.v_accGM != null) append(String.format(Locale.US, "%.6f", r.v_accGM)) else append("")
                append(",")
                if (r.accAgeSec != null) append(String.format(Locale.US, "%.4f", r.accAgeSec)) else append("")

                append(",")
                if (r.p_gm_smooth != null) append(String.format(Locale.US, "%.5f", r.p_gm_smooth)) else append("")
                append(",")
                if (r.p_rv_smooth != null) append(String.format(Locale.US, "%.5f", r.p_rv_smooth)) else append("")
                append(",")
                if (r.p_accGM_smooth != null) append(String.format(Locale.US, "%.5f", r.p_accGM_smooth)) else append("")

                append(",")
                if (r.v_gm_smooth != null) append(String.format(Locale.US, "%.6f", r.v_gm_smooth)) else append("")
                append(",")
                if (r.s_gm_smooth != null) append(String.format(Locale.US, "%.6f", r.s_gm_smooth)) else append("")
                append(",")
                if (r.v_rv_smooth != null) append(String.format(Locale.US, "%.6f", r.v_rv_smooth)) else append("")
                append(",")
                if (r.s_rv_smooth != null) append(String.format(Locale.US, "%.6f", r.s_rv_smooth)) else append("")
                append(",")
                if (r.v_accGM_smooth != null) append(String.format(Locale.US, "%.6f", r.v_accGM_smooth)) else append("")
                append(",")
                if (r.s_accGM_smooth != null) append(String.format(Locale.US, "%.6f", r.s_accGM_smooth)) else append("")

                fun appendFlag(b: Boolean?) {
                    append(",")
                    if (b == null) append("") else append(if (b) "1" else "0")
                }
                appendFlag(r.flagOriStaleGM)
                appendFlag(r.flagOriStaleRV)
                appendFlag(r.flagAccStale)
                append(","); append(if (r.flagLaSat) "1" else "0")
                append(","); append(if (r.flagLaJump) "1" else "0")
                appendFlag(r.flagPgmOut)
                appendFlag(r.flagPrvOut)
                appendFlag(r.flagGravAnom)
                appendFlag(r.flagMagAnom)
                append("\n")
            }
            w.write(line); w.flush()
        }.onFailure {
            if (isLogging) android.util.Log.e("SensorLogger", "LA write failed", it)
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
                    Sensor.TYPE_LINEAR_ACCELERATION ->
                        "prevLaTsNs,laTsNs,laClockSec,laX,laY,laZ,Azimuth," +
                                "P,V,oriAgeGM,yawGMdeg," +
                                "P_rv,V_rv,oriAgeRV,yawRVdeg," +
                                "P_accGM,V_accGM,accAgeSec," +
                                "Pgm_smooth,Prv_smooth,PaccGM_smooth," +
                                "Vgm_smooth,Sgm_smooth,Vrv_smooth,Srv_smooth,VaccGM_smooth,SaccGM_smooth," +
                                "isOriStaleGM,isOriStaleRV,isAccStale,isLaSat,isLaJump,isPgmOutlier,isPrvOutlier,isGravAnom,isMagAnom\n"
                    else -> "timestamp,val0,val1,val2\n"
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

    // ==============================
    // Post-processing: detrend velocities and rewrite LA CSV
    // ==============================
    private fun postProcessLaCsv(distanceMeters: Double) {
        val laFile = File(logDir, "run_${sessionTimestamp}_la.csv")
        if (!laFile.exists()) return

        data class Reg(var n: Int = 0, var sumT: Double = 0.0, var sumT2: Double = 0.0, var sumV: Double = 0.0, var sumTV: Double = 0.0) {
            fun add(t: Double, v: Double) { n++; sumT += t; sumT2 += t*t; sumV += v; sumTV += t*v }
            fun slopeIntercept(): Pair<Double, Double> {
                if (n < 2) return 0.0 to 0.0
                val denom = n * sumT2 - sumT * sumT
                if (abs(denom) < 1e-12) return 0.0 to (sumV / n)
                val m = (n * sumTV - sumT * sumV) / denom
                val b = (sumV - m * sumT) / n
                return m to b
            }
        }

        // First pass: find column indexes and compute regressions
        val header = laFile.bufferedReader().use { it.readLine() } ?: return
        val cols = header.split(",")
        val idxTime = cols.indexOf("laClockSec")
        val idxV      = cols.indexOf("V")
        val idxVrv    = cols.indexOf("V_rv")
        val idxVacc   = cols.indexOf("V_accGM")
        val idxVgmSm  = cols.indexOf("Vgm_smooth")
        val idxVrvSm  = cols.indexOf("Vrv_smooth")
        val idxVaccSm = cols.indexOf("VaccGM_smooth")

        if (idxTime < 0) return

        val regV = if (idxV >= 0) Reg() else null
        val regVrv = if (idxVrv >= 0) Reg() else null
        val regVacc = if (idxVacc >= 0) Reg() else null
        val regVgmSm = if (idxVgmSm >= 0) Reg() else null
        val regVrvSm = if (idxVrvSm >= 0) Reg() else null
        val regVaccSm = if (idxVaccSm >= 0) Reg() else null

        var lastT = 0.0

        laFile.bufferedReader().use { br ->
            var line = br.readLine() // header already read above; read again to align
            // consume first line
            // read the real first data line now
            while (true) {
                line = br.readLine() ?: break
                if (line.isEmpty()) continue
                val parts = line.split(",")
                if (idxTime >= parts.size) continue
                val tStr = parts[idxTime]
                if (tStr.isEmpty()) continue
                val t = tStr.toDoubleOrNull() ?: continue
                lastT = t

                fun upd(idx: Int, reg: Reg?) {
                    if (reg == null || idx < 0 || idx >= parts.size) return
                    val s = parts[idx]
                    if (s.isNotEmpty()) {
                        val v = s.toDoubleOrNull()
                        if (v != null) reg.add(t, v)
                    }
                }
                upd(idxV, regV)
                upd(idxVrv, regVrv)
                upd(idxVacc, regVacc)
                upd(idxVgmSm, regVgmSm)
                upd(idxVrvSm, regVrvSm)
                upd(idxVaccSm, regVaccSm)
            }
        }

        val (mV, bV) = regV?.slopeIntercept() ?: (0.0 to 0.0)
        val (mVrv, bVrv) = regVrv?.slopeIntercept() ?: (0.0 to 0.0)
        val (mVacc, bVacc) = regVacc?.slopeIntercept() ?: (0.0 to 0.0)
        val (mVgmSm, bVgmSm) = regVgmSm?.slopeIntercept() ?: (0.0 to 0.0)
        val (mVrvSm, bVrvSm) = regVrvSm?.slopeIntercept() ?: (0.0 to 0.0)
        val (mVaccSm, bVaccSm) = regVaccSm?.slopeIntercept() ?: (0.0 to 0.0)

        val durationSec = if (lastT > 0.0) lastT else 0.0
        val avgVel = if (durationSec > 0.0) distanceMeters / durationSec else 0.0

        // Second pass: rewrite file with extra columns, then append footer rows
        val tmpFile = File(logDir, "run_${sessionTimestamp}_la_tmp.csv")
        laFile.bufferedReader().use { br ->
            tmpFile.bufferedWriter().use { bw ->
                // header
                val newHeader = header + ",V_detrend,V_rv_detrend,V_accGM_detrend,Vgm_smooth_detrend,Vrv_smooth_detrend,VaccGM_smooth_detrend\n"
                bw.write(newHeader)

                var line: String?
                // write rows
                while (true) {
                    line = br.readLine() ?: break
                    if (line!!.isEmpty()) continue
                    val parts = line!!.split(",")
                    // copy original row
                    bw.write(line!!)
                    // append detrended columns
                    val t = if (idxTime < parts.size) parts[idxTime].toDoubleOrNull() ?: Double.NaN else Double.NaN

                    fun corr(idx: Int, m: Double, b: Double): String {
                        if (idx < 0 || idx >= parts.size) return ""
                        val s = parts[idx]
                        if (s.isEmpty() || t.isNaN()) return ""
                        val v = s.toDoubleOrNull() ?: return ""
                        val vCorr = v - (m * t + b) + avgVel
                        return String.format(Locale.US, "%.6f", vCorr)
                    }

                    bw.write(",")
                    bw.write(corr(idxV, mV, bV))
                    bw.write(",")
                    bw.write(corr(idxVrv, mVrv, bVrv))
                    bw.write(",")
                    bw.write(corr(idxVacc, mVacc, bVacc))
                    bw.write(",")
                    bw.write(corr(idxVgmSm, mVgmSm, bVgmSm))
                    bw.write(",")
                    bw.write(corr(idxVrvSm, mVrvSm, bVrvSm))
                    bw.write(",")
                    bw.write(corr(idxVaccSm, mVaccSm, bVaccSm))
                    bw.write("\n")
                }

                // Footer rows: #slope and #intercept aligned under new columns
                val baseColsCount = header.split(",").size
                val allColsCount = baseColsCount + 6

                fun footerRow(label: String, values: List<Double>): String {
                    val cells = MutableList(allColsCount) { "" }
                    cells[0] = label
                    // place slopes/intercepts under the 6 new detrend columns
                    for ((i, v) in values.withIndex()) {
                        val colIndex = baseColsCount + i
                        cells[colIndex] = String.format(Locale.US, "%.9f", v)
                    }
                    return cells.joinToString(",") + "\n"
                }

                bw.write(
                    footerRow("#slope", listOf(mV, mVrv, mVacc, mVgmSm, mVrvSm, mVaccSm))
                )
                bw.write(
                    footerRow("#intercept", listOf(bV, bVrv, bVacc, bVgmSm, bVrvSm, bVaccSm))
                )
            }
        }

        // Replace original file atomically
        if (!laFile.delete()) {
            android.util.Log.w("SensorLogger", "Failed to delete original LA CSV; attempting overwrite")
        }
        if (!tmpFile.renameTo(laFile)) {
            android.util.Log.e("SensorLogger", "Failed to rename temp LA CSV to original name")
        }
    }

    // ==============================
    // Companion object
    // ==============================
    companion object {
        const val EXTRA_AZIMUTH_DEG = "extra_azimuth_deg"
    }
}
