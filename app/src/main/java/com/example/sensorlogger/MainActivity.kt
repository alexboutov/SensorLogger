package com.example.sensorlogger

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.Path
import android.os.Build
import android.content.pm.ActivityInfo
import android.os.Bundle
import android.view.View
import android.view.WindowManager
import androidx.appcompat.app.AlertDialog
import android.view.inputmethod.EditorInfo
import android.view.inputmethod.InputMethodManager
import androidx.appcompat.app.AppCompatActivity
import com.example.sensorlogger.databinding.ActivityMainBinding

private const val PREFS_NAME = "sensor_logger_prefs"
private const val PREF_AZIMUTH = "azimuth_deg"
private const val PREF_DISTANCE = "distance_meters"
private const val PREF_TARGET_TIME = "target_time_sec"
private const val PREF_FAST_THRESHOLD = "fast_threshold_pct"
private const val PREF_SLOW_THRESHOLD = "slow_threshold_pct"

// Default values
private const val DEFAULT_DISTANCE = "13.716"    // 15 yards in meters
private const val DEFAULT_TARGET_TIME = "10.0"   // seconds
private const val DEFAULT_FAST_THRESHOLD = "20"    // % above target velocity
private const val DEFAULT_SLOW_THRESHOLD = "15"    // % below target velocity

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private val prefs by lazy { getSharedPreferences(PREFS_NAME, MODE_PRIVATE) }

    // Local UI state for the toggle button
    private var isLogging = false
    
    // Receiver for pace feedback
    private val paceResultReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context?, intent: Intent?) {
            if (intent?.action == SensorLoggerService.ACTION_PACE_RESULT) {
                // Get chart data arrays
                val chartTimes = intent.getDoubleArrayExtra(SensorLoggerService.EXTRA_CHART_TIMES)
                val chartVelocities = intent.getDoubleArrayExtra(SensorLoggerService.EXTRA_CHART_VELOCITIES)
                val vTarget = intent.getDoubleExtra(SensorLoggerService.EXTRA_V_TARGET, 1.372)
                val vStdDev = intent.getDoubleExtra(SensorLoggerService.EXTRA_V_STDDEV, 0.2)
                val status = intent.getStringExtra(SensorLoggerService.EXTRA_PACE_STATUS) ?: return
                val targetTime = intent.getDoubleExtra(SensorLoggerService.EXTRA_TARGET_TIME, 0.0)
                val detectedTime = intent.getDoubleExtra(SensorLoggerService.EXTRA_DETECTED_TIME, 0.0)
                val distance = intent.getDoubleExtra(SensorLoggerService.EXTRA_DISTANCE, 0.0)
                
                showPaceFeedback(status, targetTime, detectedTime, distance, 
                    chartTimes, chartVelocities, vTarget, vStdDev)
            }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)
        
        // Register pace result receiver
        val filter = IntentFilter(SensorLoggerService.ACTION_PACE_RESULT)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            registerReceiver(paceResultReceiver, filter, Context.RECEIVER_NOT_EXPORTED)
        } else {
            registerReceiver(paceResultReceiver, filter)
        }

        // Restore last saved values (or defaults)
        val lastAzimuth = prefs.getString(PREF_AZIMUTH, "0.0") ?: "0.0"
        val lastDistance = prefs.getString(PREF_DISTANCE, DEFAULT_DISTANCE) ?: DEFAULT_DISTANCE
        val lastTargetTime = prefs.getString(PREF_TARGET_TIME, DEFAULT_TARGET_TIME) ?: DEFAULT_TARGET_TIME
        
        binding.azimuthEditText.setText(lastAzimuth)
        binding.distanceEditText.setText(lastDistance)
        binding.targetTimeEditText.setText(lastTargetTime)
        
        val lastFastThreshold = prefs.getString(PREF_FAST_THRESHOLD, DEFAULT_FAST_THRESHOLD) ?: DEFAULT_FAST_THRESHOLD
        val lastSlowThreshold = prefs.getString(PREF_SLOW_THRESHOLD, DEFAULT_SLOW_THRESHOLD) ?: DEFAULT_SLOW_THRESHOLD
        binding.fastThresholdEditText.setText(lastFastThreshold)
        binding.slowThresholdEditText.setText(lastSlowThreshold)

        // Initial label for the single button
        updateStartStopLabel()

        // Save + hide keyboard when user taps "Done" on azimuth
        binding.azimuthEditText.setOnEditorActionListener { v, actionId, _ ->
            if (actionId == EditorInfo.IME_ACTION_NEXT || actionId == EditorInfo.IME_ACTION_DONE) {
                saveAllFields()
                if (actionId == EditorInfo.IME_ACTION_DONE) {
                    v.clearFocus()
                    hideKeyboard()
                }
                true
            } else {
                false
            }
        }

        // Save + hide keyboard when user taps "Done" on distance
        binding.distanceEditText.setOnEditorActionListener { v, actionId, _ ->
            if (actionId == EditorInfo.IME_ACTION_NEXT || actionId == EditorInfo.IME_ACTION_DONE) {
                saveAllFields()
                if (actionId == EditorInfo.IME_ACTION_DONE) {
                    v.clearFocus()
                    hideKeyboard()
                }
                true
            } else {
                false
            }
        }

        // Save + hide keyboard when user taps "Done" on target time
        binding.targetTimeEditText.setOnEditorActionListener { v, actionId, _ ->
            if (actionId == EditorInfo.IME_ACTION_DONE) {
                saveAllFields()
                v.clearFocus()
                hideKeyboard()
                true
            } else {
                false
            }
        }

        // Hide keyboard when focus leaves any field
        binding.azimuthEditText.setOnFocusChangeListener { _, hasFocus ->
            if (!hasFocus) hideKeyboard()
        }
        binding.distanceEditText.setOnFocusChangeListener { _, hasFocus ->
            if (!hasFocus) hideKeyboard()
        }
        binding.targetTimeEditText.setOnFocusChangeListener { _, hasFocus ->
            if (!hasFocus) hideKeyboard()
        }
        
        binding.fastThresholdEditText.setOnFocusChangeListener { _, hasFocus ->
            if (!hasFocus) hideKeyboard()
        }
        
        binding.slowThresholdEditText.setOnFocusChangeListener { _, hasFocus ->
            if (!hasFocus) hideKeyboard()
        }

        // Single START/STOP toggle
        binding.startStopButton.setOnClickListener {
            // Always save and hide keyboard on button press
            saveAllFields()
            hideKeyboard()
            binding.azimuthEditText.clearFocus()
            binding.distanceEditText.clearFocus()
            binding.targetTimeEditText.clearFocus()

            if (!isLogging) {
                // START: parse values and pass to service
                val azText = binding.azimuthEditText.text?.toString()?.trim().orEmpty()
                val azimuthDeg = azText.toDoubleOrNull()?.let { v ->
                    // normalize into [0, 360)
                    ((v % 360.0) + 360.0) % 360.0
                } ?: 0.0

                val distText = binding.distanceEditText.text?.toString()?.trim().orEmpty()
                val distanceMeters = distText.toDoubleOrNull() ?: DEFAULT_DISTANCE.toDouble()

                val timeText = binding.targetTimeEditText.text?.toString()?.trim().orEmpty()
                val targetTimeSec = timeText.toDoubleOrNull() ?: DEFAULT_TARGET_TIME.toDouble()

                val intent = Intent(this, SensorLoggerService::class.java).apply {
                    putExtra(SensorLoggerService.EXTRA_AZIMUTH_DEG, azimuthDeg)
                    putExtra(SensorLoggerService.EXTRA_DISTANCE_METERS, distanceMeters)
                    putExtra(SensorLoggerService.EXTRA_TARGET_TIME_SEC, targetTimeSec)
                }
                startService(intent)

                isLogging = true
                updateStartStopLabel()
            } else {
                // STOP
                stopService(Intent(this, SensorLoggerService::class.java))
                isLogging = false
                updateStartStopLabel()
            }
        }
    }

    override fun onPause() {
        super.onPause()
        saveAllFields()
    }

    private fun saveAllFields() {
        val azText = binding.azimuthEditText.text?.toString()?.trim().orEmpty()
        val distText = binding.distanceEditText.text?.toString()?.trim().orEmpty()
        val timeText = binding.targetTimeEditText.text?.toString()?.trim().orEmpty()
        val fastText = binding.fastThresholdEditText.text?.toString()?.trim().orEmpty()
        val slowText = binding.slowThresholdEditText.text?.toString()?.trim().orEmpty()
        
        prefs.edit()
            .putString(PREF_AZIMUTH, azText)
            .putString(PREF_DISTANCE, distText)
            .putString(PREF_TARGET_TIME, timeText)
            .putString(PREF_FAST_THRESHOLD, fastText)
            .putString(PREF_SLOW_THRESHOLD, slowText)
            .apply()
    }

    private fun updateStartStopLabel() {
        binding.startStopButton.text = if (isLogging) "STOP" else "START"
    }

    private fun hideKeyboard() {
        val imm = getSystemService(Context.INPUT_METHOD_SERVICE) as InputMethodManager
        val view = currentFocus ?: binding.azimuthEditText
        imm.hideSoftInputFromWindow(view.windowToken, 0)
    }
    
    override fun onDestroy() {
        super.onDestroy()
        try {
            unregisterReceiver(paceResultReceiver)
        } catch (e: Exception) {
            // Receiver might not be registered
        }
    }
    
    private fun showPaceFeedback(
        status: String, 
        targetTime: Double, 
        detectedTime: Double, 
        distance: Double,
        chartTimes: DoubleArray?,
        chartVelocities: DoubleArray?,
        vTarget: Double,
        vStdDev: Double
    ) {
        if (chartTimes != null && chartVelocities != null && chartTimes.isNotEmpty()) {
            // Always show velocity chart (with failure overlay if needed)
            showVelocityChart(status, distance, targetTime, detectedTime, chartTimes, chartVelocities, vTarget, vStdDev)
        } else {
            // Fallback to simple dialog if no chart data
            showFailureDialog(status, targetTime, detectedTime)
        }
    }
    
    private fun showFailureDialog(status: String, targetTime: Double, detectedTime: Double) {
        val dialogView = layoutInflater.inflate(android.R.layout.simple_list_item_1, null)
        val textView = dialogView.findViewById<android.widget.TextView>(android.R.id.text1)
        
        val message = if (status == "TOO_FAST") {
            "TOO FAST - Please Repeat"
        } else {
            "TOO SLOW - Please Repeat"
        }
        
        textView.text = "$message\n\nTarget: ${String.format("%.1f", targetTime)} sec\nActual: ${String.format("%.1f", detectedTime)} sec"
        textView.setTextColor(Color.RED)
        textView.textSize = 18f
        textView.gravity = android.view.Gravity.CENTER
        textView.setPadding(40, 40, 40, 40)
        
        AlertDialog.Builder(this)
            .setView(dialogView)
            .setPositiveButton("OK", null)
            .show()
    }
    
    private fun showVelocityChart(
        status: String,
        distance: Double,
        targetTime: Double,
        detectedTime: Double,
        times: DoubleArray,
        velocities: DoubleArray,
        vTarget: Double,
        vStdDev: Double
    ) {
        // Get threshold values from UI
        val fastThreshold = binding.fastThresholdEditText.text?.toString()?.toDoubleOrNull() ?: DEFAULT_FAST_THRESHOLD.toDouble()
        val slowThreshold = binding.slowThresholdEditText.text?.toString()?.toDoubleOrNull() ?: DEFAULT_SLOW_THRESHOLD.toDouble()
        
        // Launch full-screen ChartActivity instead of dialog
        val intent = Intent(this, ChartActivity::class.java).apply {
            putExtra(ChartActivity.EXTRA_STATUS, status)
            putExtra(ChartActivity.EXTRA_DISTANCE, distance)
            putExtra(ChartActivity.EXTRA_TARGET_TIME, targetTime)
            putExtra(ChartActivity.EXTRA_DETECTED_TIME, detectedTime)
            putExtra(ChartActivity.EXTRA_TIMES, times)
            putExtra(ChartActivity.EXTRA_VELOCITIES, velocities)
            putExtra(ChartActivity.EXTRA_V_TARGET, vTarget)
            putExtra(ChartActivity.EXTRA_V_STDDEV, vStdDev)
            putExtra(ChartActivity.EXTRA_FAST_THRESHOLD_PCT, fastThreshold)
            putExtra(ChartActivity.EXTRA_SLOW_THRESHOLD_PCT, slowThreshold)
        }
        startActivity(intent)
    }
    
    // Custom View for velocity chart
    private inner class VelocityChartView(
        context: Context,
        private val status: String,
        private val times: DoubleArray,
        private val velocities: DoubleArray,
        private val vTarget: Double,
        private val vStdDev: Double,
        private val distance: Double,
        private val targetTime: Double,
        private val detectedTime: Double
    ) : View(context) {
        
        private val linePaint = Paint().apply {
            color = Color.BLUE
            strokeWidth = 4f
            style = Paint.Style.STROKE
            isAntiAlias = true
        }
        
        private val targetPaint = Paint().apply {
            color = Color.parseColor("#228B22")  // Forest green
            strokeWidth = 3f
            style = Paint.Style.STROKE
            isAntiAlias = true
        }
        
        private val sigmaPaint = Paint().apply {
            color = Color.parseColor("#FFA500")  // Orange
            strokeWidth = 2f
            style = Paint.Style.STROKE
            isAntiAlias = true
            pathEffect = android.graphics.DashPathEffect(floatArrayOf(10f, 10f), 0f)
        }
        
        private val bandPaint = Paint().apply {
            color = Color.parseColor("#33FFA500")  // Transparent orange
            style = Paint.Style.FILL
        }
        
        private val textPaint = Paint().apply {
            color = Color.BLACK
            textSize = 36f
            isAntiAlias = true
        }
        
        private val titlePaint = Paint().apply {
            color = Color.parseColor("#228B22")
            textSize = 48f
            isAntiAlias = true
            isFakeBoldText = true
        }
        
        private val failureTitlePaint = Paint().apply {
            color = Color.RED
            textSize = 48f
            isAntiAlias = true
            isFakeBoldText = true
        }
        
        private val failureOverlayPaint = Paint().apply {
            color = Color.argb(64, 255, 0, 0)  // 25% opacity red
            style = Paint.Style.FILL
        }
        
        private val failureTextPaint = Paint().apply {
            color = Color.RED
            textSize = 42f
            isAntiAlias = true
            isFakeBoldText = true
            textAlign = Paint.Align.CENTER
        }
        
        private val distancePaint = Paint().apply {
            color = Color.parseColor("#228B22")
            textSize = 64f
            isAntiAlias = true
            isFakeBoldText = true
        }
        
        private val gridPaint = Paint().apply {
            color = Color.LTGRAY
            strokeWidth = 1f
            style = Paint.Style.STROKE
        }
        
        override fun onMeasure(widthMeasureSpec: Int, heightMeasureSpec: Int) {
            val width = MeasureSpec.getSize(widthMeasureSpec)
            val height = (width * 0.6).toInt()  // 5:3 aspect ratio for landscape feel
            setMeasuredDimension(width, height)
        }
        
        override fun onDraw(canvas: Canvas) {
            super.onDraw(canvas)
            
            if (times.isEmpty() || velocities.isEmpty()) return
            
            val padding = 80f
            val topPadding = 140f  // Extra space for title
            val chartWidth = width - 2 * padding
            val chartHeight = height - topPadding - padding
            
            // Draw title and distance
            canvas.drawText("PACE ACHIEVED!", padding, 50f, titlePaint)
            canvas.drawText("Distance: ${String.format("%.1f", distance)}m", padding, 110f, distancePaint)
            canvas.drawText("Time: ${String.format("%.1f", detectedTime)}s", padding + 400f, 110f, textPaint)
            
            // Calculate scales
            val tMax = times.maxOrNull() ?: 1.0
            val vMin = (vTarget - 2.5 * vStdDev).coerceAtMost(velocities.minOrNull() ?: 0.0)
            val vMax = (vTarget + 2.5 * vStdDev).coerceAtLeast(velocities.maxOrNull() ?: 2.0)
            val vRange = vMax - vMin
            
            fun xPos(t: Double) = padding + (t / tMax * chartWidth).toFloat()
            fun yPos(v: Double) = topPadding + chartHeight - ((v - vMin) / vRange * chartHeight).toFloat()
            
            // Draw ±1σ band
            val bandPath = Path()
            bandPath.moveTo(xPos(0.0), yPos(vTarget + vStdDev))
            bandPath.lineTo(xPos(tMax), yPos(vTarget + vStdDev))
            bandPath.lineTo(xPos(tMax), yPos(vTarget - vStdDev))
            bandPath.lineTo(xPos(0.0), yPos(vTarget - vStdDev))
            bandPath.close()
            canvas.drawPath(bandPath, bandPaint)
            
            // Draw grid
            for (i in 0..4) {
                val y = topPadding + chartHeight * i / 4
                canvas.drawLine(padding, y, padding + chartWidth, y, gridPaint)
            }
            for (i in 0..5) {
                val x = padding + chartWidth * i / 5
                canvas.drawLine(x, topPadding, x, topPadding + chartHeight, gridPaint)
            }
            
            // Draw target line
            canvas.drawLine(xPos(0.0), yPos(vTarget), xPos(tMax), yPos(vTarget), targetPaint)
            
            // Draw ±1σ lines
            canvas.drawLine(xPos(0.0), yPos(vTarget + vStdDev), xPos(tMax), yPos(vTarget + vStdDev), sigmaPaint)
            canvas.drawLine(xPos(0.0), yPos(vTarget - vStdDev), xPos(tMax), yPos(vTarget - vStdDev), sigmaPaint)
            
            // Draw velocity line
            val path = Path()
            path.moveTo(xPos(times[0]), yPos(velocities[0]))
            for (i in 1 until times.size) {
                path.lineTo(xPos(times[i]), yPos(velocities[i]))
            }
            canvas.drawPath(path, linePaint)
            
            // Draw axis labels
            textPaint.textSize = 28f
            canvas.drawText("Time (s)", padding + chartWidth / 2 - 50, height - 10f, textPaint)
            canvas.drawText("0", padding - 10, topPadding + chartHeight + 30, textPaint)
            canvas.drawText(String.format("%.1f", tMax), padding + chartWidth - 30, topPadding + chartHeight + 30, textPaint)
            
            // Velocity labels
            canvas.drawText(String.format("%.1f", vTarget), padding - 70, yPos(vTarget) + 10, textPaint)
            textPaint.color = Color.parseColor("#FFA500")
            canvas.drawText("+1σ", padding + chartWidth + 10, yPos(vTarget + vStdDev) + 10, textPaint)
            canvas.drawText("-1σ", padding + chartWidth + 10, yPos(vTarget - vStdDev) + 10, textPaint)
            
            // Draw failure overlay if not successful
            val isSuccess = status == "VALID"
            if (!isSuccess) {
                // Semi-transparent overlay on chart area
                canvas.drawRect(padding, topPadding, padding + chartWidth, topPadding + chartHeight, failureOverlayPaint)
                
                // Failure message in center of chart
                val centerX = padding + chartWidth / 2
                val centerY = topPadding + chartHeight / 2
                canvas.drawText("Failed to make target", centerX, centerY - 20f, failureTextPaint)
                val targetTimeStr = String.format("%.1f", targetTime)
                canvas.drawText("time of $targetTimeStr seconds", centerX, centerY + 30f, failureTextPaint)
            }
        }
    }
}
