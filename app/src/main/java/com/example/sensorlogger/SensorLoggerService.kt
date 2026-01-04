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
    private val P_OUTLIER_THRESH = 10.0          // m/s^2 along heading
    private val GRAV_NORM = SensorManager.STANDARD_GRAVITY.toDouble() // ~9.80665
    private val GRAV_TOL = 1.5                   // m/s^2 tolerance
    private val MAG_MIN_UT = 20.0                // microTesla lower bound
    private val MAG_MAX_UT = 70.0                // microTesla upper bound

    // Smoothing window
    private val N_SMOOTH = 20
    private val WINDOW_SIZE = 2 * N_SMOOTH + 1

    // Post-processing config
    private var DIST_METERS = 13.716             // distance in meters (set from UI)
    private var TARGET_TIME_SEC = 10.0           // target lap time in seconds (set from UI)
    private val TIME_TOLERANCE_SEC = 1.0         // acceptable deviation from target

    // Gyro-prediction config (LA-triggered engine)
    private val PRED_WINDOW_SEC = 0.25           // keep ~250 ms of gyro/orientation history
    private val MAX_PRED_MS = 50L                // cap forward prediction/span to 50 ms

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
        val flagMagAnom: Boolean?,

        // ===== NEW: LA-triggered predicted projection (gyro-predicted orientation) =====
        val p_pred: Double? = null,
        val v_pred: Double? = null,
        val oriTsNsUsed: Long? = null,
        val oriAgeSec_pred: Double? = null,
        val gyroSpanMs_pred: Double? = null,
        val yawDeg_pred: Double? = null,
        val pitchDeg_pred: Double? = null,
        val rollDeg_pred: Double? = null,
        val vCorrRT: Double? = null,
        val vCorrRTFiltered: Double? = null
    )

    // ==============================
    // Variables (class scope)
    // ==============================
    private val sensorManager by lazy { getSystemService(Context.SENSOR_SERVICE) as SensorManager }

    private lateinit var logDir: File
    private lateinit var sessionTimestamp: String

    private val fileWriters = mutableMapOf<Int, FileWriter>()
    private var combinedWriter: FileWriter? = null
    private var laCsvFile: File? = null  // memorize the exact LA CSV path

    private val latestLA = Snapshot()
    private val latestACC = Snapshot()
    private val latestGRAV = Snapshot()
    private val latestMAG = Snapshot()

    // Rotation providers
    private val gravity = FloatArray(3)
    private val magnetic = FloatArray(3)
    private var hasGravity = false
    private var hasMagnetic = false
    private var lastValidMagnetic: FloatArray? = null  // Last known good magnetometer reading
    private var lastMagTsNs: Long = 0L                 // Timestamp of last mag reading
    private val MAG_MIN_MAGNITUDE = 20.0f   // μT - below this = interference or sensor error
    private val MAG_MAX_MAGNITUDE = 80.0f   // μT - above this = metal/electronics nearby
    private val MAG_MAX_CHANGE_RATE = 300.0f // μT/sec - faster than this = sudden interference
    private var magValidCount = 0L          // Count of valid mag readings
    private var magInvalidCount = 0L        // Count of rejected mag readings
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

    // ===== NEW: LA-triggered engine (gyro-predicted orientation) =====
//    private val engineLA = ProjectionEngineLA(PRED_WINDOW_SEC, MAX_PRED_MS)
    private val engineLA = ProjectionEngineORI(PRED_WINDOW_SEC)

    private var vAccum_pred: Double = 0.0

    // Real-time velocity correction variables
    private var vRawSampleCount = 0L   // Count of V_raw samples during walking
    private var vRawSum = 0.0          // Running sum of V_raw (vAccum_gm)
    private var vAvgKnownRT = 1.372    // V_avg from declared pace (set from UI on start)
    
    // DC-blocking high-pass filter for drift removal (minimal phase shift)
    // y[n] = α * (y[n-1] + x[n] - x[n-1])
    // α = 0.995 → cutoff ~0.15 Hz at 200 Hz, removes drift while preserving walking signal
    private val DC_BLOCK_ALPHA = 0.995
    private var dcBlockPrevInput = 0.0   // x[n-1]
    private var dcBlockPrevOutput = 0.0  // y[n-1]

    // Telemetry placeholders (optional class-level mirrors)
    private var lastOriTsUsed: Long = 0L
    private var lastOriAgeSec: Double = Double.NaN
    private var lastGyroSpanMs: Double = Double.NaN
    private var lastYawDeg: Double = Double.NaN
    private var lastPitchDeg: Double = Double.NaN
    private var lastRollDeg: Double = Double.NaN

    /** Concurrency & lifecycle guards */
    private val ioErrorHandler = CoroutineExceptionHandler { _, e ->
        android.util.Log.d("SensorLogger", "I/O after stop (ignored): ${e.message}")
    }
    private val serviceJob = SupervisorJob()
    private val writeExecutor = Executors.newSingleThreadExecutor()
    private val ioScope = CoroutineScope(writeExecutor.asCoroutineDispatcher() + serviceJob + ioErrorHandler)

    @Volatile private var isLogging = false
    
    // Pace detection results (set during post-processing)
    @Volatile private var lastPaceStatus: String = ""
    @Volatile private var lastDetectedTime: Double = 0.0
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

        // Read azimuth from UI
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
        
        // Read distance and target time from UI
        DIST_METERS = intent?.getDoubleExtra(EXTRA_DISTANCE_METERS, 13.716) ?: 13.716
        TARGET_TIME_SEC = intent?.getDoubleExtra(EXTRA_TARGET_TIME_SEC, 10.0) ?: 10.0
        
        android.util.Log.d("SensorLogger", "UI azimuthDeg=$azimuthDeg°  dirFx=$dirFx dirFy=$dirFy")
        android.util.Log.d("SensorLogger", "UI distance=${DIST_METERS}m  targetTime=${TARGET_TIME_SEC}s")

        // Reset state
        vAccum_gm = 0.0
        vAccum_rv = 0.0
        vAccum_accGM = 0.0
        vAccum_pred = 0.0
        
        // Reset real-time correction variables
        vRawSampleCount = 0L
        vRawSum = 0.0
        vAvgKnownRT = DIST_METERS / TARGET_TIME_SEC  // Calculate from UI settings
        
        // Reset DC-block filter state
        dcBlockPrevInput = 0.0
        dcBlockPrevOutput = 0.0
        
        // Reset magnetometer validity counters
        lastValidMagnetic = null
        lastMagTsNs = 0L
        magValidCount = 0L
        magInvalidCount = 0L
        
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

        engineLA.clear()
        lastOriTsUsed = 0L
        lastOriAgeSec = Double.NaN
        lastGyroSpanMs = Double.NaN
        lastYawDeg = Double.NaN
        lastPitchDeg = Double.NaN
        lastRollDeg = Double.NaN

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
        runCatching { postProcessLaCsv(laCsvFile, DIST_METERS) }
            .onFailure { android.util.Log.e("SensorLogger", "post-process failed", it) }
        
        // === Post-process: constrained-time velocity correction ===
        runCatching { postProcessVelocityCorrection(laCsvFile, DIST_METERS, TARGET_TIME_SEC, TIME_TOLERANCE_SEC) }
            .onFailure { android.util.Log.e("SensorLogger", "velocity correction failed", it) }

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
            Sensor.TYPE_ROTATION_VECTOR,
            Sensor.TYPE_GYROSCOPE              // NEW: buffer-only (no CSV)
        ).forEach { type ->
            sensorManager.getDefaultSensor(type)?.let {
                sensorManager.registerListener(this, it, SensorManager.SENSOR_DELAY_FASTEST)
            }
        }
    }

    private fun unregisterSensors() {
        sensorManager.unregisterListener(this)
    }
    /**
     * Check if magnetometer reading is valid.
     * Returns true if the reading should be used, false if it should be rejected.
     */
    private fun isMagnetometerValid(values: FloatArray, timestampNs: Long): Boolean {
        // Calculate magnitude: sqrt(x² + y² + z²)
        val magnitude = kotlin.math.sqrt(
            values[0] * values[0] +
                    values[1] * values[1] +
                    values[2] * values[2]
        )

        // Check 1: Magnitude within Earth's field range
        if (magnitude < MAG_MIN_MAGNITUDE || magnitude > MAG_MAX_MAGNITUDE) {
            return false
        }

        // Check 2: Rate of change not too fast (if we have a previous reading)
        val lastMag = lastValidMagnetic
        if (lastMag != null && lastMagTsNs > 0) {
            val dtSec = (timestampNs - lastMagTsNs) * 1e-9
            if (dtSec > 0.001) {  // Avoid division by very small numbers
                val dx = values[0] - lastMag[0]
                val dy = values[1] - lastMag[1]
                val dz = values[2] - lastMag[2]
                val changeMagnitude = kotlin.math.sqrt(dx*dx + dy*dy + dz*dz)
                val changeRate = changeMagnitude / dtSec.toFloat()

                if (changeRate > MAG_MAX_CHANGE_RATE) {
                    return false
                }
            }
        }

        return true
    }
    // ==============================
    // onSensorChanged processing
    // ==============================
    override fun onSensorChanged(event: SensorEvent) {
        if (!isLogging) return

        val sensorType = event.sensor.type
        val sensorTsNs = event.timestamp
        val values = event.values.copyOf()

        // Fast path: cache the latest raw vectors/timestamps
        when (sensorType) {
            Sensor.TYPE_GRAVITY -> {
                System.arraycopy(values, 0, gravity, 0, 3)
                hasGravity = true
                latestGRAV.ts = sensorTsNs
                System.arraycopy(values, 0, latestGRAV.v, 0, 3)
            }
            Sensor.TYPE_MAGNETIC_FIELD -> {
                // Apply validity gating before accepting magnetometer reading
                if (isMagnetometerValid(values, sensorTsNs)) {
                    System.arraycopy(values, 0, magnetic, 0, 3)
                    hasMagnetic = true
                    latestMAG.ts = sensorTsNs
                    System.arraycopy(values, 0, latestMAG.v, 0, 3)
                    lastValidMagnetic = values.copyOf()
                    lastMagTsNs = sensorTsNs
                    magValidCount++
                } else {
                    // Reject bad reading, keep using last valid magnetic
                    magInvalidCount++
                }
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
                            // GM -> world (use R^T for device->world transform)
                            val accWorldX_gm = rotMatrixGM[0]*laX + rotMatrixGM[3]*laY + rotMatrixGM[6]*laZ
                            val accWorldY_gm = rotMatrixGM[1]*laX + rotMatrixGM[4]*laY + rotMatrixGM[7]*laZ
                            latestP_gm = accWorldX_gm * dirFx + accWorldY_gm * dirFy
                        }

                        if (haveRV) {
                            // RV -> world (use R^T for device->world transform)
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
                        
                        // ===== Real-time corrected velocity =====
                        var vCorrRT = 0.0
                        var vCorrRTFiltered = 0.0
                        if (haveGM) {
                            vRawSampleCount++
                            vRawSum += vAccum_gm
                            val vRawMeanRunning = if (vRawSampleCount > 0) vRawSum / vRawSampleCount else 0.0
                            vCorrRT = vAvgKnownRT + (vAccum_gm - vRawMeanRunning)
                            
                            // Apply DC-blocking high-pass filter to remove residual drift
                            // Formula: y[n] = α * (y[n-1] + x[n] - x[n-1])
                            val vInput = vCorrRT - vAvgKnownRT  // Center around zero for filtering
                            vCorrRTFiltered = DC_BLOCK_ALPHA * (dcBlockPrevOutput + vInput - dcBlockPrevInput)
                            dcBlockPrevInput = vInput
                            dcBlockPrevOutput = vCorrRTFiltered
                            vCorrRTFiltered += vAvgKnownRT  // Add back the target average
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

                        // ======== NEW: LA-triggered predicted orientation ========
                        var local_pPred: Double? = null
                        var local_vPred: Double? = null
                        var local_oriTs: Long? = null
                        var local_oriAge: Double? = null
                        var local_gyroSpan: Double? = null
                        var local_yaw: Double? = null
                        var local_pitch: Double? = null
                        var local_roll: Double? = null
                        // Ensure the ORI engine sees an orientation sample even if GRAV/MAG/RV events are queued behind LA
                        if (hasRV) {
                            val qRV = OrientationUtils.fromRotVec(rotVec)
                            engineLA.onOri(OriSample(ts = latestRotTsNs, q = qRV))
                        } else if (hasGravity && hasMagnetic) {
                            // Use GM fallback when RV not present
                            SensorManager.getRotationMatrix(rotMatrixGM, null, gravity, magnetic)
                            val qGM = OrientationUtils.fromRotationMatrix(rotMatrixGM)
                            val tOri = maxOf(latestGRAV.ts, latestMAG.ts)
                            engineLA.onOri(OriSample(ts = tOri, q = qGM))
                        }

                        val pred = engineLA.onLa(curr, laX, laY, laZ, dirFx, dirFy)
                        if (pred != null) {
                            local_pPred = pred.pPred
                            if (prevForLine != 0L) vAccum_pred = engineLA.addToVelocity(dtSec, pred.pPred)
                            local_vPred = vAccum_pred
                            local_oriTs = pred.oriTsUsed
                            local_oriAge = pred.oriAgeSec
                            local_gyroSpan = pred.gyroSpanMs
                            local_yaw = pred.yawDeg
                            local_pitch = pred.pitchDeg
                            local_roll = pred.rollDeg

                            lastOriTsUsed = pred.oriTsUsed
                            lastOriAgeSec = pred.oriAgeSec
                            lastGyroSpanMs = pred.gyroSpanMs
                            lastYawDeg = pred.yawDeg
                            lastPitchDeg = pred.pitchDeg
                            lastRollDeg = pred.rollDeg
                        }

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
                            flagMagAnom = flagMagAnom,
                            // NEW fields
                            p_pred = local_pPred,
                            v_pred = local_vPred,
                            oriTsNsUsed = local_oriTs,
                            oriAgeSec_pred = local_oriAge,
                            gyroSpanMs_pred = local_gyroSpan,
                            yawDeg_pred = local_yaw,
                            pitchDeg_pred = local_pitch,
                            rollDeg_pred = local_roll,
                            vCorrRT = if (haveGM) vCorrRT else null,
                            vCorrRTFiltered = if (haveGM) vCorrRTFiltered else null
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
                        // writeSimple(sensorType, sensorTsNs, values)  // Disabled - only logging LA
                    }
                    Sensor.TYPE_GRAVITY -> {
                        // writeSimple(sensorType, sensorTsNs, values)  // Disabled - only logging LA
                        // push orientation sample (GM) to engine when both are available
                        if (hasGravity && hasMagnetic) {
                            SensorManager.getRotationMatrix(rotMatrixGM, null, gravity, magnetic)
                            val qGM = OrientationUtils.fromRotationMatrix(rotMatrixGM)
                            val tOri = maxOf(latestGRAV.ts, latestMAG.ts)
                            engineLA.onOri(OriSample(ts = tOri, q = qGM))
                        }
                    }
                    Sensor.TYPE_MAGNETIC_FIELD -> {
                        // writeSimple(sensorType, sensorTsNs, values)  // Disabled - only logging LA
                        if (hasGravity && hasMagnetic) {
                            SensorManager.getRotationMatrix(rotMatrixGM, null, gravity, magnetic)
                            val qGM = OrientationUtils.fromRotationMatrix(rotMatrixGM)
                            val tOri = maxOf(latestGRAV.ts, latestMAG.ts)
                            engineLA.onOri(OriSample(ts = tOri, q = qGM))
                        }
                    }
                    Sensor.TYPE_ROTATION_VECTOR -> {
                        // writeSimple(sensorType, sensorTsNs, values)  // Disabled - only logging LA
                        val qRV = OrientationUtils.fromRotVec(rotVec)
                        engineLA.onOri(OriSample(ts = latestRotTsNs, q = qRV))
                    }
                    Sensor.TYPE_GYROSCOPE -> {
                        // Buffer gyro for prediction (no CSV)
                        engineLA.onGyro(GyroSample(sensorTsNs, values[0], values[1], values[2]))
                    }
                }

                if (!isLogging || !writersOpen) return@synchronized

                // ---- Combined snapshot CSV (DISABLED) ----
                /* // Disabled - only logging LA
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
                */ // End disabled combined CSV block
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
                // Core timing & LA
                append(r.prevLaTsNs); append(",")
                append(r.laTsNs); append(",")
                append(String.format(Locale.US, "%.3f", r.laClockSec)); append(",")
                append(String.format(Locale.US, "%.3f", r.laX)); append(",")
                append(String.format(Locale.US, "%.3f", r.laY)); append(",")
                append(String.format(Locale.US, "%.3f", r.laZ)); append(",")

                // Azimuth (deg) from UI
                append(String.format(Locale.US, "%.3f", r.azimuthDeg)); append(",")

                // Primary P,V (GM)
                if (r.haveGM) append(String.format(Locale.US, "%.3f", r.p_gm)) else append("")
                append(",")
                if (r.haveGM) append(String.format(Locale.US, "%.3f", r.v_gm)) else append("")

                append(",")
                // oriAgeGM, yawGMdeg (unchanged formatting)
                if (r.haveGM && r.oriAgeGM != null) append(String.format(Locale.US, "%.4f", r.oriAgeGM)) else append("")
                append(",")
                if (r.haveGM && r.yawGMdeg != null) append(String.format(Locale.US, "%.1f", r.yawGMdeg)) else append("")

                append(",")
                // P_rv, V_rv
                if (r.haveRV) append(String.format(Locale.US, "%.3f", r.p_rv)) else append("")
                append(",")
                if (r.haveRV) append(String.format(Locale.US, "%.3f", r.v_rv)) else append("")

                append(",")
                // oriAgeRV, yawRVdeg
                if (r.haveRV && r.oriAgeRV != null) append(String.format(Locale.US, "%.4f", r.oriAgeRV)) else append("")
                append(",")
                if (r.haveRV && r.yawRVdeg != null) append(String.format(Locale.US, "%.1f", r.yawRVdeg)) else append("")

                append(",")
                // P_accGM, V_accGM, accAgeSec
                if (r.p_accGM != null) append(String.format(Locale.US, "%.3f", r.p_accGM)) else append("")
                append(",")
                if (r.v_accGM != null) append(String.format(Locale.US, "%.3f", r.v_accGM)) else append("")
                append(",")
                if (r.accAgeSec != null) append(String.format(Locale.US, "%.3f", r.accAgeSec)) else append("")

                // ===== Smoothed P (centered SMA) =====
                append(",")
                if (r.p_gm_smooth != null) append(String.format(Locale.US, "%.3f", r.p_gm_smooth)) else append("")
                append(",")
                if (r.p_rv_smooth != null) append(String.format(Locale.US, "%.3f", r.p_rv_smooth)) else append("")
                append(",")
                if (r.p_accGM_smooth != null) append(String.format(Locale.US, "%.3f", r.p_accGM_smooth)) else append("")

                // ===== Smoothed Velocity and Distance (displacement) =====
                append(",")
                if (r.v_gm_smooth != null) append(String.format(Locale.US, "%.3f", r.v_gm_smooth)) else append("")
                append(",")
                if (r.s_gm_smooth != null) append(String.format(Locale.US, "%.3f", r.s_gm_smooth)) else append("")
                append(",")
                if (r.v_rv_smooth != null) append(String.format(Locale.US, "%.3f", r.v_rv_smooth)) else append("")
                append(",")
                if (r.s_rv_smooth != null) append(String.format(Locale.US, "%.3f", r.s_rv_smooth)) else append("")
                append(",")
                if (r.v_accGM_smooth != null) append(String.format(Locale.US, "%.3f", r.v_accGM_smooth)) else append("")
                append(",")
                if (r.s_accGM_smooth != null) append(String.format(Locale.US, "%.3f", r.s_accGM_smooth)) else append("")

                // ===== Flags (0/1), blanks when N/A =====
                fun appendFlag(b: Boolean?) {
                    append(",")
                    if (b == null) append("") else append(if (b) "1" else "0")
                }

                appendFlag(r.flagOriStaleGM) // isOriStaleGM
                appendFlag(r.flagOriStaleRV) // isOriStaleRV
                appendFlag(r.flagAccStale)   // isAccStale
                append(","); append(if (r.flagLaSat) "1" else "0")   // isLaSat
                append(","); append(if (r.flagLaJump) "1" else "0")  // isLaJump
                appendFlag(r.flagPgmOut)     // isPgmOutlier
                appendFlag(r.flagPrvOut)     // isPrvOutlier
                appendFlag(r.flagGravAnom)   // isGravAnom
                appendFlag(r.flagMagAnom)    // isMagAnom

                // ===== NEW: LA-triggered predicted columns =====
                append(","); if (r.p_pred != null) append(String.format(Locale.US, "%.3f", r.p_pred)) else append("")
                append(","); if (r.v_pred != null) append(String.format(Locale.US, "%.3f", r.v_pred)) else append("")
                append(","); if (r.oriTsNsUsed != null) append(r.oriTsNsUsed) else append("")
                append(","); if (r.oriAgeSec_pred != null) append(String.format(Locale.US, "%.4f", r.oriAgeSec_pred)) else append("")
                append(","); if (r.gyroSpanMs_pred != null) append(String.format(Locale.US, "%.2f", r.gyroSpanMs_pred)) else append("")
                append(","); if (r.yawDeg_pred != null) append(String.format(Locale.US, "%.1f", r.yawDeg_pred)) else append("")
                append(","); if (r.pitchDeg_pred != null) append(String.format(Locale.US, "%.1f", r.pitchDeg_pred)) else append("")
                append(","); if (r.rollDeg_pred != null) append(String.format(Locale.US, "%.1f", r.rollDeg_pred)) else append("")
                append(","); if (r.vCorrRT != null) append(String.format(Locale.US, "%.3f", r.vCorrRT)) else append("")
                append(","); if (r.vCorrRTFiltered != null) append(String.format(Locale.US, "%.3f", r.vCorrRTFiltered)) else append("")

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
                // We intentionally do not create a CSV for gyro
                else                           -> "other"
            }

            val fileName = "run_${sessionTimestamp}_$typeSuffix.csv"
            val file = File(logDir, fileName)

            // Memorize LA file path once
            if (sensorType == Sensor.TYPE_LINEAR_ACCELERATION && laCsvFile == null) {
                laCsvFile = file
            }

            val writer = FileWriter(file, true)
            if (file.length() == 0L) {
                val header = when (sensorType) {
                    // Note: header names unchanged; only appended new columns at end for LA
                    Sensor.TYPE_LINEAR_ACCELERATION ->
                        "prevLaTsNs,laTsNs,laClockSec,laX,laY,laZ,Azimuth," +
                                "P,V,oriAgeGM,yawGMdeg," +
                                "P_rv,V_rv,oriAgeRV,yawRVdeg," +
                                "P_accGM,V_accGM,accAgeSec," +
                                "Pgm_smooth,Prv_smooth,PaccGM_smooth," +
                                "Vgm_smooth,Sgm_smooth,Vrv_smooth,Srv_smooth,VaccGM_smooth,SaccGM_smooth," +
                                "isOriStaleGM,isOriStaleRV,isAccStale,isLaSat,isLaJump,isPgmOutlier,isPrvOutlier,isGravAnom,isMagAnom," +
                                "P_pred,V_pred,oriTsNsUsed,oriAgeSec_pred,gyroSpanMs_pred,yawDeg_pred,pitchDeg_pred,rollDeg_pred,V_corr_rt,V_corr_rt_filt\n"
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
    // Post-processing: detrend velocities and replace LA CSV in place
    // ==============================
    private fun postProcessLaCsv(laFileRef: File?, distanceMeters: Double) {
        val laFile = laFileRef ?: return
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
        val idxTime  = cols.indexOf("laClockSec")
        val idxV     = cols.indexOf("V")
        val idxVrv   = cols.indexOf("V_rv")
        val idxVacc  = cols.indexOf("V_accGM")
        val idxVgmSm = cols.indexOf("Vgm_smooth")
        val idxVrvSm = cols.indexOf("Vrv_smooth")
        val idxVaccSm= cols.indexOf("VaccGM_smooth")
        if (idxTime < 0) return

        val regV      = if (idxV      >= 0) Reg() else null
        val regVrv    = if (idxVrv    >= 0) Reg() else null
        val regVacc   = if (idxVacc   >= 0) Reg() else null
        val regVgmSm  = if (idxVgmSm  >= 0) Reg() else null
        val regVrvSm  = if (idxVrvSm  >= 0) Reg() else null
        val regVaccSm = if (idxVaccSm >= 0) Reg() else null

        var lastT = 0.0

        laFile.bufferedReader().use { br ->
            // skip header
            br.readLine()
            while (true) {
                val line = br.readLine() ?: break
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

        val (mV, bV)         = regV?.slopeIntercept() ?: (0.0 to 0.0)
        val (mVrv, bVrv)     = regVrv?.slopeIntercept() ?: (0.0 to 0.0)
        val (mVacc, bVacc)   = regVacc?.slopeIntercept() ?: (0.0 to 0.0)
        val (mVgmSm, bVgmSm) = regVgmSm?.slopeIntercept() ?: (0.0 to 0.0)
        val (mVrvSm, bVrvSm) = regVrvSm?.slopeIntercept() ?: (0.0 to 0.0)
        val (mVaccSm, bVaccSm) = regVaccSm?.slopeIntercept() ?: (0.0 to 0.0)

        val durationSec = if (lastT > 0.0) lastT else 0.0
        val avgVel = if (durationSec > 0.0) distanceMeters / durationSec else 0.0

        // Second pass: rewrite file with extra columns, then append footer rows
        val tmpFile = File(laFile.parentFile, laFile.nameWithoutExtension + "_tmp.csv")
        laFile.bufferedReader().use { br ->
            tmpFile.bufferedWriter().use { bw ->
                // header
                val newHeader = header + ",V_detrend,V_rv_detrend,V_accGM_detrend,Vgm_smooth_detrend,Vrv_smooth_detrend,VaccGM_smooth_detrend\n"
                bw.write(newHeader)

                // skip original header before iterating rows
                br.readLine()

                while (true) {
                    val line = br.readLine() ?: break
                    if (line.isEmpty()) continue
                    val parts = line.split(",")

                    // copy original row
                    bw.write(line)

                    // time for correction
                    val t = if (idxTime < parts.size) parts[idxTime].toDoubleOrNull() ?: Double.NaN else Double.NaN

                    fun corr(idx: Int, m: Double, b: Double): String {
                        if (idx < 0 || idx >= parts.size) return ""
                        val s = parts[idx]
                        if (s.isEmpty() || t.isNaN()) return ""
                        val v = s.toDoubleOrNull() ?: return ""
                        val vCorr = v - (m * t + b) + avgVel
                        return String.format(Locale.US, "%.3f", vCorr)
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

                // Footer rows: #slope and #intercept (keep high precision)
                val baseColsCount = header.split(",").size
                val allColsCount = baseColsCount + 6

                fun footerRow(label: String, values: List<Double>): String {
                    val cells = MutableList(allColsCount) { "" }
                    cells[0] = label
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

        // Replace original file atomically in-place
        if (!laFile.delete()) {
            android.util.Log.w("SensorLogger", "Failed to delete original LA CSV; attempting overwrite")
        }
        if (!tmpFile.renameTo(laFile)) {
            android.util.Log.e("SensorLogger", "Failed to rename temp LA CSV to original name")
        }
    }

    // ==============================
    // Zero-phase high-pass filter (no time shift)
    // Implements forward-backward Butterworth filter
    // ==============================
    private fun zeroPhaseHighPass(data: DoubleArray, cutoffHz: Double, sampleRateHz: Double): DoubleArray {
        if (data.size < 12) return data  // Need minimum samples for filtering
        
        // 2nd order Butterworth high-pass coefficients
        val omega = kotlin.math.tan(kotlin.math.PI * cutoffHz / sampleRateHz)
        val omega2 = omega * omega
        val sqrt2 = kotlin.math.sqrt(2.0)
        val denom = 1.0 + sqrt2 * omega + omega2
        
        val b0 = 1.0 / denom
        val b1 = -2.0 / denom
        val b2 = 1.0 / denom
        val a1 = 2.0 * (omega2 - 1.0) / denom
        val a2 = (1.0 - sqrt2 * omega + omega2) / denom
        
        // Forward pass
        val forward = DoubleArray(data.size)
        forward[0] = data[0]
        forward[1] = data[1]
        for (i in 2 until data.size) {
            forward[i] = b0 * data[i] + b1 * data[i-1] + b2 * data[i-2] - a1 * forward[i-1] - a2 * forward[i-2]
        }
        
        // Backward pass (reverse, filter, reverse again)
        val backward = DoubleArray(data.size)
        backward[data.size - 1] = forward[data.size - 1]
        backward[data.size - 2] = forward[data.size - 2]
        for (i in (data.size - 3) downTo 0) {
            backward[i] = b0 * forward[i] + b1 * forward[i+1] + b2 * forward[i+2] - a1 * backward[i+1] - a2 * backward[i+2]
        }
        
        return backward
    }

    // ==============================
    // Post-processing: Constrained-time velocity correction
    // ==============================
    private fun postProcessVelocityCorrection(
        laFileRef: File?,
        distanceMeters: Double,
        targetTimeSec: Double,
        toleranceSec: Double
    ) {
        val laFile = laFileRef ?: return
        if (!laFile.exists()) return

        val WALK_THRESHOLD = 0.3  // m/s - threshold to detect motion start/end

        // First pass: read data and detect walking phase
        val header = laFile.bufferedReader().use { it.readLine() } ?: return
        val cols = header.split(",")
        val idxTime = cols.indexOf("laClockSec")
        val idxV = cols.indexOf("V")
        val idxVrv = cols.indexOf("V_rv")
        val idxVpred = cols.indexOf("V_pred")
        
        if (idxTime < 0 || idxV < 0) {
            android.util.Log.w("SensorLogger", "Required columns not found for velocity correction")
            return
        }

        data class VelData(val t: Double, val v: Double, val vrv: Double?, val vpred: Double?)
        val dataRows = mutableListOf<VelData>()

        laFile.bufferedReader().use { br ->
            br.readLine() // skip header
            while (true) {
                val line = br.readLine() ?: break
                if (line.isEmpty() || line.startsWith("#")) continue
                val parts = line.split(",")
                if (idxTime >= parts.size) continue
                
                val t = parts[idxTime].toDoubleOrNull() ?: continue
                val v = parts.getOrNull(idxV)?.toDoubleOrNull() ?: continue
                val vrv = parts.getOrNull(idxVrv)?.toDoubleOrNull()
                val vpred = parts.getOrNull(idxVpred)?.toDoubleOrNull()
                
                dataRows.add(VelData(t, v, vrv, vpred))
            }
        }

        if (dataRows.size < 10) return

        // Detect walking/swimming phase
        // Start: first time |V| exceeds threshold
        var walkStartIdx = 0
        for (i in dataRows.indices) {
            if (kotlin.math.abs(dataRows[i].v) > WALK_THRESHOLD) {
                walkStartIdx = i
                break
            }
        }

        // End: last time V differs significantly from final V
        val finalV = dataRows.last().v
        var walkEndIdx = dataRows.size - 1
        for (i in (dataRows.size - 1) downTo 0) {
            if (kotlin.math.abs(dataRows[i].v - finalV) > WALK_THRESHOLD) {
                walkEndIdx = i
                break
            }
        }

        // Ensure valid range
        if (walkEndIdx <= walkStartIdx) {
            android.util.Log.w("SensorLogger", "Could not detect valid walking phase")
            return
        }

        val walkStartTime = dataRows[walkStartIdx].t
        val walkEndTime = dataRows[walkEndIdx].t
        val detectedDuration = walkEndTime - walkStartTime

        android.util.Log.d("SensorLogger", 
            "Detected walking phase: ${String.format(java.util.Locale.US, "%.2f", walkStartTime)}s to ${String.format(java.util.Locale.US, "%.2f", walkEndTime)}s (duration: ${String.format(java.util.Locale.US, "%.2f", detectedDuration)}s)")

        // Check if detected duration is within tolerance of TARGET time
        val timeDiff = kotlin.math.abs(detectedDuration - targetTimeSec)
        
        // Determine pace status
        val paceStatus = when {
            timeDiff <= toleranceSec -> "VALID"
            detectedDuration < targetTimeSec -> "TOO_FAST"
            else -> "TOO_SLOW"
        }
        
        // Store for broadcast
        lastPaceStatus = paceStatus
        lastDetectedTime = detectedDuration
        
        
        
        if (timeDiff > toleranceSec) {
            android.util.Log.w("SensorLogger", 
                "WARNING: Detected duration (${String.format(java.util.Locale.US, "%.1f", detectedDuration)}s) differs from target (${String.format(java.util.Locale.US, "%.1f", targetTimeSec)}s) by ${String.format(java.util.Locale.US, "%.1f", timeDiff)}s. Exceeds tolerance of +/-${String.format(java.util.Locale.US, "%.1f", toleranceSec)}s. Please repeat the lap at target pace.")
            // Still apply correction but add warning to file
        }

        // Calculate velocities using TARGET time (declared pace), not detected time
        val vAvgKnown = distanceMeters / targetTimeSec
        
        // Calculate raw velocity mean over the WALKING PHASE ONLY
        val walkingData = dataRows.subList(walkStartIdx, walkEndIdx + 1)
        val vMean = walkingData.map { it.v }.average()
        val vrvMean = walkingData.mapNotNull { it.vrv }.let { if (it.isNotEmpty()) it.average() else null }
        val vpredMean = walkingData.mapNotNull { it.vpred }.let { if (it.isNotEmpty()) it.average() else null }

        // Calculate integrated distance from corrected velocity (walking phase only)
        var calculatedDistance = 0.0
        for (i in (walkStartIdx + 1)..walkEndIdx) {
            val dt = dataRows[i].t - dataRows[i-1].t
            val vCorr1 = vAvgKnown + (dataRows[i-1].v - vMean)
            val vCorr2 = vAvgKnown + (dataRows[i].v - vMean)
            calculatedDistance += (vCorr1 + vCorr2) / 2.0 * dt
        }

        // Prepare chart data (downsample if needed for UI performance)
        val maxChartPoints = 500
        val step = if (walkingData.size > maxChartPoints) walkingData.size / maxChartPoints else 1
        val chartTimes = DoubleArray((walkingData.size + step - 1) / step)
        val chartVelocities = DoubleArray(chartTimes.size)
        val chartStartTime = walkingData.firstOrNull()?.t ?: 0.0
        
        // First pass: compute unfiltered V_corr
        for (i in chartTimes.indices) {
            val srcIdx = (i * step).coerceAtMost(walkingData.size - 1)
            chartTimes[i] = walkingData[srcIdx].t - chartStartTime  // Normalize to start at 0
            chartVelocities[i] = vAvgKnown + (walkingData[srcIdx].v - vMean)
        }
        
        // Apply zero-phase high-pass filter to remove drift from chart display
        if (chartVelocities.size >= 12) {
            val sampleRateChart = if (chartTimes.size > 1 && chartTimes.last() > 0) {
                (chartTimes.size - 1) / chartTimes.last()
            } else 200.0
            
            // Center, filter, then restore offset
            val chartCentered = DoubleArray(chartVelocities.size) { chartVelocities[it] - vAvgKnown }
            val chartFiltered = zeroPhaseHighPass(chartCentered, 0.15, sampleRateChart)
            for (i in chartVelocities.indices) {
                chartVelocities[i] = vAvgKnown + chartFiltered[i]
            }
        }
        
        // Calculate velocity std dev for ±σ bands
        // val vCorrValues = walkingData.map { vAvgKnown + (it.v - vMean) }
        // Calculate velocity std dev for ±σ bands
        val vCorrValues = walkingData.map { vAvgKnown + (it.v - vMean) }
        val vStdDev = kotlin.math.sqrt(vCorrValues.map { (it - vAvgKnown) * (it - vAvgKnown) }.average())
        
        // Broadcast the result
        val resultIntent = android.content.Intent(ACTION_PACE_RESULT).apply {
            putExtra(EXTRA_PACE_STATUS, paceStatus)
            putExtra(EXTRA_TARGET_TIME, targetTimeSec)
            putExtra(EXTRA_DETECTED_TIME, detectedDuration)
            putExtra(EXTRA_DISTANCE, calculatedDistance)
            putExtra(EXTRA_CHART_TIMES, chartTimes)
            putExtra(EXTRA_CHART_VELOCITIES, chartVelocities)
            putExtra(EXTRA_V_TARGET, vAvgKnown)
            putExtra(EXTRA_V_STDDEV, vStdDev)
            setPackage(packageName)
        }
        sendBroadcast(resultIntent)

        android.util.Log.d("SensorLogger", 
            "Velocity correction: V_avg_target=${String.format(java.util.Locale.US, "%.3f", vAvgKnown)} m/s, V_raw_mean=${String.format(java.util.Locale.US, "%.3f", vMean)} m/s, correction=${String.format(java.util.Locale.US, "%.3f", vAvgKnown - vMean)} m/s")

        // Compute zero-phase filtered V_corr for the walking phase
        val vCorrArray = DoubleArray(dataRows.size) { i ->
            if (i in walkStartIdx..walkEndIdx) {
                vAvgKnown + (dataRows[i].v - vMean)
            } else {
                dataRows[i].v
            }
        }
        
        // Extract walking phase for filtering
        val walkVCorr = vCorrArray.slice(walkStartIdx..walkEndIdx).toDoubleArray()
        val sampleRate = if (detectedDuration > 0) walkingData.size / detectedDuration else 200.0
        
        // Apply zero-phase high-pass filter (0.15 Hz cutoff) to remove drift
        val walkVCorrCentered = DoubleArray(walkVCorr.size) { walkVCorr[it] - vAvgKnown }
        val walkVCorrFiltered = zeroPhaseHighPass(walkVCorrCentered, 0.15, sampleRate)
        
        // Reconstruct full array with filtered values
        val vCorrFiltered = DoubleArray(dataRows.size) { i ->
            if (i in walkStartIdx..walkEndIdx) {
                val walkIdx = i - walkStartIdx
                vAvgKnown + walkVCorrFiltered[walkIdx]
            } else {
                vCorrArray[i]
            }
        }
        
        // Second pass: rewrite file with corrected velocities
        val tmpFile = File(laFile.parentFile, laFile.nameWithoutExtension + "_vcorr.csv")
        var rowIndex = 0
        val paceValid = timeDiff <= toleranceSec

        laFile.bufferedReader().use { br ->
            tmpFile.bufferedWriter().use { bw ->
                // Write header with new columns
                val newHeader = header + ",V_corr,V_corr_filt,V_rv_corr,V_pred_corr,V_avg_known,walk_phase,pace_valid\n"
                bw.write(newHeader)

                br.readLine() // skip original header
                while (true) {
                    val line = br.readLine() ?: break
                    if (line.isEmpty()) continue
                    
                    // Pass through comment/footer rows
                    if (line.startsWith("#")) {
                        bw.write(line)
                        bw.write(",,,,,,\n")
                        continue
                    }

                    bw.write(line)

                    // Add corrected velocities
                    if (rowIndex < dataRows.size) {
                        val d = dataRows[rowIndex]
                        val inWalkPhase = rowIndex in walkStartIdx..walkEndIdx
                        
                        // Only apply correction during walking phase
                        val vCorr = if (inWalkPhase) vAvgKnown + (d.v - vMean) else d.v
                        val vCorrFilt = if (inWalkPhase && rowIndex < vCorrFiltered.size) vCorrFiltered[rowIndex] else vCorr
                        val vrvCorr = if (inWalkPhase && d.vrv != null && vrvMean != null) 
                            vAvgKnown + (d.vrv - vrvMean) else d.vrv
                        val vpredCorr = if (inWalkPhase && d.vpred != null && vpredMean != null) 
                            vAvgKnown + (d.vpred - vpredMean) else d.vpred

                        bw.write(",")
                        bw.write(String.format(java.util.Locale.US, "%.3f", vCorr))
                        bw.write(",")
                        bw.write(String.format(java.util.Locale.US, "%.3f", vCorrFilt))
                        bw.write(",")
                        if (vrvCorr != null) bw.write(String.format(java.util.Locale.US, "%.3f", vrvCorr))
                        bw.write(",")
                        if (vpredCorr != null) bw.write(String.format(java.util.Locale.US, "%.3f", vpredCorr))
                        bw.write(",")
                        bw.write(String.format(java.util.Locale.US, "%.3f", vAvgKnown))
                        bw.write(",")
                        bw.write(if (inWalkPhase) "1" else "0")
                        bw.write(",")
                        bw.write(if (paceValid) "1" else "0")
                        bw.write("\n")
                        
                        rowIndex++
                    } else {
                        bw.write(",,,,,,\n")
                    }
                }

                // Add summary footer
                val summaryColCount = cols.size + 6
                
                // Correction info
                val corrCells = MutableList(summaryColCount) { "" }
                corrCells[0] = "#V_correction"
                corrCells[corrCells.size - 6] = String.format(java.util.Locale.US, "%.6f", vAvgKnown - vMean)
                bw.write(corrCells.joinToString(",") + "\n")
                
                // Target time
                val targetCells = MutableList(summaryColCount) { "" }
                targetCells[0] = "#target_time_sec"
                targetCells[1] = String.format(java.util.Locale.US, "%.3f", targetTimeSec)
                bw.write(targetCells.joinToString(",") + "\n")
                
                // Detected time
                val detectedCells = MutableList(summaryColCount) { "" }
                detectedCells[0] = "#detected_time_sec"
                detectedCells[1] = String.format(java.util.Locale.US, "%.3f", detectedDuration)
                bw.write(detectedCells.joinToString(",") + "\n")
                
                // Pace valid flag
                val validCells = MutableList(summaryColCount) { "" }
                validCells[0] = "#pace_valid"
                validCells[1] = if (paceValid) "YES" else "NO - REPEAT LAP"
                bw.write(validCells.joinToString(",") + "\n")
                
                // V_avg_known
                val avgCells = MutableList(summaryColCount) { "" }
                avgCells[0] = "#V_avg_known"
                avgCells[1] = String.format(java.util.Locale.US, "%.3f", vAvgKnown)
                bw.write(avgCells.joinToString(",") + "\n")
                // Magnetometer validity stats
                val magValidCells = MutableList(summaryColCount) { "" }
                magValidCells[0] = "#mag_valid_count"
                magValidCells[1] = magValidCount.toString()
                bw.write(magValidCells.joinToString(",") + "\n")

                val magInvalidCells = MutableList(summaryColCount) { "" }
                magInvalidCells[0] = "#mag_invalid_count"
                magInvalidCells[1] = magInvalidCount.toString()
                bw.write(magInvalidCells.joinToString(",") + "\n")

                val magRejectCells = MutableList(summaryColCount) { "" }
                magRejectCells[0] = "#mag_rejection_rate"
                val rejectRate = if (magValidCount + magInvalidCount > 0) 100.0 * magInvalidCount / (magValidCount + magInvalidCount) else 0.0
                magRejectCells[1] = String.format(java.util.Locale.US, "%.1f%%", rejectRate)
                bw.write(magRejectCells.joinToString(",") + "\n")
            }
        }

        // Replace original with corrected file
        if (!laFile.delete()) {
            android.util.Log.w("SensorLogger", "Failed to delete original LA CSV for velocity correction")
        }
        if (!tmpFile.renameTo(laFile)) {
            android.util.Log.e("SensorLogger", "Failed to rename velocity-corrected LA CSV")
        } else {
            val statusMsg = if (paceValid) "Pace validated OK" else "WARNING: Pace outside tolerance - repeat lap"
            android.util.Log.d("SensorLogger", "Velocity correction applied. $statusMsg")
        }
    }

    // ==============================
    // Companion object
    // ==============================
    companion object {
        const val EXTRA_AZIMUTH_DEG = "extra_azimuth_deg"
        const val EXTRA_DISTANCE_METERS = "extra_distance_meters"
        const val EXTRA_TARGET_TIME_SEC = "extra_target_time_sec"
        
        // Broadcast action for pace feedback
        const val ACTION_PACE_RESULT = "com.example.sensorlogger.PACE_RESULT"
        const val EXTRA_PACE_STATUS = "pace_status"  // "VALID", "TOO_FAST", "TOO_SLOW"
        const val EXTRA_TARGET_TIME = "target_time"
        const val EXTRA_DETECTED_TIME = "detected_time"
        const val EXTRA_DISTANCE = "distance"  // Calculated distance (for valid pace only)
        const val EXTRA_CHART_TIMES = "chart_times"  // Time array for chart
        const val EXTRA_CHART_VELOCITIES = "chart_velocities"  // V_corr array for chart
        const val EXTRA_V_TARGET = "v_target"  // Target average velocity
        const val EXTRA_V_STDDEV = "v_stddev"  // Velocity standard deviation
    }
}
