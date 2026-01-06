package com.example.sensorlogger

import java.io.File
import kotlin.math.abs

/**
 * Detects swim/walk phase boundaries using acceleration and velocity data.
 * 
 * Based on swimming research:
 * - PMC4732051: "wall push off is characterised by a rapid increase in acceleration 
 *   over a short interval, such as a 1 g rise (9.81 m/s²) over a 0.1s duration"
 * - PMC8688996: "Push maximum velocity: the highest velocity during the lap is 
 *   generated at start"
 * - FORM Goggles / Garmin: "rapid acceleration off the wall (leg push-off) and a 
 *   short streamline (pause in arm motion)"
 * 
 * Detection strategies:
 * - START: Find FIRST significant acceleration burst (not max), walk back to quiet period
 * - END: Find FIRST sustained quiet period (low variance) after minimum duration
 */
class SwimPhaseDetector(
    // Start detection parameters
    private val pushoffAccelThreshold: Double = 1.5,   // m/s² - acceleration threshold for push-off
    private val fallbackVelThreshold: Double = 0.3,    // m/s - fallback velocity threshold
    private val quietThreshold: Double = 0.5,          // m/s² - below this = quiet (not moving)
    
    // End detection parameters  
    private val varianceWindowSize: Int = 100,         // 0.5s window at 200Hz
    private val varianceStepSize: Int = 50,            // 0.25s steps
    private val walkVarianceThreshold: Double = 0.8,   // Variance above this = walking
    private val velocityEndThreshold: Double = 0.2,    // m/s - fallback for end detection
    private val minQuietWindows: Int = 4,              // Require 4 consecutive quiet windows (~1s)
    
    // Timing constraints
    private val minPhaseDurationSec: Double = 3.0,     // Minimum expected phase duration
    private val sampleRate: Double = 200.0             // Samples per second
) {
    companion object {
        private const val TAG = "SwimPhaseDetector"
    }

    /**
     * Data class representing a single row of velocity/acceleration data
     */
    data class DataPoint(
        val t: Double,        // timestamp (seconds)
        val v: Double,        // velocity (m/s)
        val p: Double?,       // forward acceleration (m/s²), nullable
        val vrv: Double?,     // rotation vector velocity
        val vpred: Double?    // predicted velocity
    )

    /**
     * Result of phase detection
     */
    data class PhaseResult(
        val startIdx: Int,
        val endIdx: Int,
        val startTime: Double,
        val endTime: Double,
        val duration: Double,
        val maxAccel: Double,
        val detectionMethod: String,  // "acceleration" or "velocity_fallback"
        val endDetectionMethod: String // "variance" or "velocity_fallback"
    )

    /**
     * Parse CSV file and return list of data points
     */
    fun parseDataFile(laFile: File): List<DataPoint>? {
        if (!laFile.exists()) return null

        val header = laFile.bufferedReader().use { it.readLine() } ?: return null
        val cols = header.split(",")
        val idxTime = cols.indexOf("laClockSec")
        val idxV = cols.indexOf("V")
        val idxP = cols.indexOf("P")
        val idxVrv = cols.indexOf("V_rv")
        val idxVpred = cols.indexOf("V_pred")

        if (idxTime < 0 || idxV < 0) {
            android.util.Log.w(TAG, "Required columns not found (laClockSec, V)")
            return null
        }

        val dataRows = mutableListOf<DataPoint>()

        laFile.bufferedReader().use { br ->
            br.readLine() // skip header
            while (true) {
                val line = br.readLine() ?: break
                if (line.isEmpty() || line.startsWith("#")) continue
                val parts = line.split(",")
                if (idxTime >= parts.size) continue

                val t = parts[idxTime].toDoubleOrNull() ?: continue
                val v = parts.getOrNull(idxV)?.toDoubleOrNull() ?: continue
                val p = parts.getOrNull(idxP)?.toDoubleOrNull()
                val vrv = parts.getOrNull(idxVrv)?.toDoubleOrNull()
                val vpred = parts.getOrNull(idxVpred)?.toDoubleOrNull()

                dataRows.add(DataPoint(t, v, p, vrv, vpred))
            }
        }

        return if (dataRows.size >= 10) dataRows else null
    }

    /**
     * Detect the start and end of the swim/walk phase
     */
    fun detectPhase(dataRows: List<DataPoint>): PhaseResult? {
        if (dataRows.size < 10) return null

        // Detect start
        val (startIdx, maxAccel, startMethod) = detectStart(dataRows)
        
        // Detect end (with minimum duration constraint)
        val (endIdx, endMethod) = detectEnd(dataRows, startIdx)

        // Validate range
        if (endIdx <= startIdx) {
            android.util.Log.w(TAG, "Invalid phase: endIdx=$endIdx <= startIdx=$startIdx")
            return null
        }

        val startTime = dataRows[startIdx].t
        val endTime = dataRows[endIdx].t
        val duration = endTime - startTime

        android.util.Log.d(TAG, 
            "Phase detected: ${String.format(java.util.Locale.US, "%.2f", startTime)}s to " +
            "${String.format(java.util.Locale.US, "%.2f", endTime)}s " +
            "(duration: ${String.format(java.util.Locale.US, "%.2f", duration)}s)")

        return PhaseResult(
            startIdx = startIdx,
            endIdx = endIdx,
            startTime = startTime,
            endTime = endTime,
            duration = duration,
            maxAccel = maxAccel,
            detectionMethod = startMethod,
            endDetectionMethod = endMethod
        )
    }

    // ========================================================================
    // START DETECTION
    // Strategy: Find FIRST significant acceleration burst, then walk back to quiet
    // This differs from "max" because walking has multiple strong accelerations
    // ========================================================================
    private fun detectStart(dataRows: List<DataPoint>): Triple<Int, Double, String> {
        val hasAccelData = dataRows.any { it.p != null && it.p != 0.0 }

        if (hasAccelData) {
            // Compute rolling mean of |P| to smooth out noise
            val rollingAbsP = computeRollingMean(dataRows, 20) // ~0.1s window
            
            // Find FIRST point where rolling |P| exceeds threshold
            // This indicates the start of significant motion (push-off or first step)
            var firstHighAccelIdx = -1
            var maxAccel = 0.0
            
            for (i in rollingAbsP.indices) {
                if (rollingAbsP[i] > pushoffAccelThreshold) {
                    firstHighAccelIdx = i
                    // Track the max accel in this burst for logging
                    for (j in i until minOf(i + 100, dataRows.size)) {
                        val p = dataRows[j].p ?: 0.0
                        if (p > maxAccel) maxAccel = p
                    }
                    break
                }
            }
            
            if (firstHighAccelIdx >= 0) {
                // Walk backward to find where motion actually started (quiet period before)
                var startIdx = firstHighAccelIdx
                for (i in firstHighAccelIdx downTo 0) {
                    val absP = rollingAbsP[i]
                    if (absP < quietThreshold) {
                        startIdx = i + 1
                        break
                    }
                    if (i == 0) startIdx = 0
                }
                
                android.util.Log.d(TAG,
                    "Push-off detected: first high accel at idx=$firstHighAccelIdx, " +
                    "peak=${String.format(java.util.Locale.US, "%.2f", maxAccel)} m/s², " +
                    "start idx=$startIdx (t=${String.format(java.util.Locale.US, "%.2f", dataRows[startIdx].t)}s)")
                return Triple(startIdx, maxAccel, "acceleration")
            } else {
                // No high acceleration found - fall back to velocity
                android.util.Log.d(TAG, "No high acceleration burst found, using velocity fallback")
                val startIdx = findVelocityStart(dataRows)
                return Triple(startIdx, 0.0, "velocity_fallback")
            }
        } else {
            // No acceleration data available
            android.util.Log.d(TAG, "No acceleration data, using velocity-based detection")
            val startIdx = findVelocityStart(dataRows)
            return Triple(startIdx, 0.0, "velocity_fallback")
        }
    }

    /**
     * Compute rolling mean of |P| for smoothing
     */
    private fun computeRollingMean(dataRows: List<DataPoint>, windowSize: Int): DoubleArray {
        val result = DoubleArray(dataRows.size)
        val halfWindow = windowSize / 2
        
        for (i in dataRows.indices) {
            var sum = 0.0
            var count = 0
            for (j in maxOf(0, i - halfWindow) until minOf(dataRows.size, i + halfWindow)) {
                dataRows[j].p?.let {
                    sum += abs(it)
                    count++
                }
            }
            result[i] = if (count > 0) sum / count else 0.0
        }
        return result
    }

    private fun findVelocityStart(dataRows: List<DataPoint>): Int {
        for (i in dataRows.indices) {
            if (abs(dataRows[i].v) > fallbackVelThreshold) {
                return i
            }
        }
        return 0
    }

    // ========================================================================
    // END DETECTION
    // Strategy: Find FIRST sustained quiet period after minimum duration
    // Requires multiple consecutive low-variance windows to avoid false triggers
    // ========================================================================
    private fun detectEnd(dataRows: List<DataPoint>, startIdx: Int): Pair<Int, String> {
        // Calculate minimum end index based on minimum duration constraint
        val startTime = dataRows[startIdx].t
        val minEndTime = startTime + minPhaseDurationSec
        var minEndIdx = startIdx
        for (i in startIdx until dataRows.size) {
            if (dataRows[i].t >= minEndTime) {
                minEndIdx = i
                break
            }
        }
        
        // Compute variance of P in sliding windows, starting from minEndIdx
        val windowVariances = mutableListOf<Triple<Int, Double, Double>>()  // (centerIdx, variance, time)
        
        var windowStart = minEndIdx
        while (windowStart + varianceWindowSize < dataRows.size) {
            // Calculate variance of P values in this window
            val windowP = mutableListOf<Double>()
            for (j in windowStart until windowStart + varianceWindowSize) {
                dataRows[j].p?.let { windowP.add(it) }
            }
            
            if (windowP.size >= varianceWindowSize / 2) {
                val mean = windowP.average()
                val variance = windowP.map { (it - mean) * (it - mean) }.average()
                val centerIdx = windowStart + varianceWindowSize / 2
                val centerTime = dataRows[centerIdx].t
                windowVariances.add(Triple(centerIdx, variance, centerTime))
            }
            
            windowStart += varianceStepSize
        }
        
        // Find FIRST sustained quiet period (multiple consecutive low-variance windows)
        if (windowVariances.size >= minQuietWindows) {
            var consecutiveQuiet = 0
            var quietStartIdx = -1
            
            for (i in windowVariances.indices) {
                val (idx, variance, time) = windowVariances[i]
                
                if (variance < walkVarianceThreshold) {
                    if (consecutiveQuiet == 0) {
                        quietStartIdx = idx
                    }
                    consecutiveQuiet++
                    
                    // Found sustained quiet period
                    if (consecutiveQuiet >= minQuietWindows) {
                        android.util.Log.d(TAG, 
                            "Lap end detected (sustained quiet): " +
                            "$consecutiveQuiet consecutive quiet windows, " +
                            "var=${String.format(java.util.Locale.US, "%.2f", variance)} " +
                            "at t=${String.format(java.util.Locale.US, "%.2f", time)}s, idx=$quietStartIdx")
                        return Pair(quietStartIdx, "variance_sustained")
                    }
                } else {
                    // Reset counter - back to walking
                    consecutiveQuiet = 0
                    quietStartIdx = -1
                }
            }
            
            // If we get here, no sustained quiet period found
            // Fall back to last high-variance window
            for (i in windowVariances.indices.reversed()) {
                val (idx, variance, _) = windowVariances[i]
                if (variance >= walkVarianceThreshold) {
                    android.util.Log.d(TAG, 
                        "Lap end detected (last active): var=${String.format(java.util.Locale.US, "%.2f", variance)} at idx=$idx")
                    return Pair(idx, "variance_last_active")
                }
            }
        }
        
        // Fallback to velocity-based detection
        val finalV = dataRows.last().v
        var endIdx = dataRows.size - 1
        for (i in (dataRows.size - 1) downTo startIdx) {
            if (abs(dataRows[i].v - finalV) > velocityEndThreshold) {
                endIdx = i
                break
            }
        }
        android.util.Log.d(TAG, "Lap end detected (velocity fallback): idx=$endIdx")
        return Pair(endIdx, "velocity_fallback")
    }
}
