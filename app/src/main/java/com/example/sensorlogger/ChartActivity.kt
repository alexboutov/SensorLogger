package com.example.sensorlogger

import android.content.Context
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.Path
import android.os.Bundle
import android.view.View
import android.view.WindowManager
import androidx.appcompat.app.AppCompatActivity

/**
 * Full-screen activity for displaying velocity chart in landscape orientation.
 * Survives orientation changes and has proper back button handling.
 */
class ChartActivity : AppCompatActivity() {

    companion object {
        const val EXTRA_STATUS = "chart_status"
        const val EXTRA_DISTANCE = "chart_distance"
        const val EXTRA_TARGET_TIME = "chart_target_time"
        const val EXTRA_DETECTED_TIME = "chart_detected_time"
        const val EXTRA_TIMES = "chart_times"
        const val EXTRA_VELOCITIES = "chart_velocities"
        const val EXTRA_V_TARGET = "chart_v_target"
        const val EXTRA_V_STDDEV = "chart_v_stddev"
        const val EXTRA_FAST_THRESHOLD_PCT = "chart_fast_threshold_pct"
        const val EXTRA_SLOW_THRESHOLD_PCT = "chart_slow_threshold_pct"
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        
        // Make full screen
        window.setFlags(
            WindowManager.LayoutParams.FLAG_FULLSCREEN,
            WindowManager.LayoutParams.FLAG_FULLSCREEN
        )
        
        // Get data from intent
        val status = intent.getStringExtra(EXTRA_STATUS) ?: "VALID"
        val distance = intent.getDoubleExtra(EXTRA_DISTANCE, 0.0)
        val targetTime = intent.getDoubleExtra(EXTRA_TARGET_TIME, 10.0)
        val detectedTime = intent.getDoubleExtra(EXTRA_DETECTED_TIME, 0.0)
        val times = intent.getDoubleArrayExtra(EXTRA_TIMES) ?: doubleArrayOf()
        val velocities = intent.getDoubleArrayExtra(EXTRA_VELOCITIES) ?: doubleArrayOf()
        val vTarget = intent.getDoubleExtra(EXTRA_V_TARGET, 1.372)
        val vStdDev = intent.getDoubleExtra(EXTRA_V_STDDEV, 0.2)
        val fastThresholdPct = intent.getDoubleExtra(EXTRA_FAST_THRESHOLD_PCT, 20.0)
        val slowThresholdPct = intent.getDoubleExtra(EXTRA_SLOW_THRESHOLD_PCT, 15.0)
        
        // Set the chart view as content
        val chartView = VelocityChartView(
            this, status, times, velocities, vTarget, vStdDev, 
            distance, targetTime, detectedTime, fastThresholdPct, slowThresholdPct
        )
        setContentView(chartView)
        
        // Tap anywhere to close
        chartView.setOnClickListener {
            finish()
        }
    }
    
    override fun onBackPressed() {
        super.onBackPressed()
        finish()
    }

    // Custom View for velocity chart - full screen version
    private inner class VelocityChartView(
        context: Context,
        private val status: String,
        private val times: DoubleArray,
        private val velocities: DoubleArray,
        private val vTarget: Double,
        private val vStdDev: Double,
        private val distance: Double,
        private val targetTime: Double,
        private val detectedTime: Double,
        private val fastThresholdPct: Double,
        private val slowThresholdPct: Double
    ) : View(context) {
        
        // Calculate threshold velocities
        private val vFast = vTarget * (1.0 + fastThresholdPct / 100.0)
        private val vSlow = vTarget * (1.0 - slowThresholdPct / 100.0)
        
        private val lineNormalPaint = Paint().apply {
            color = Color.BLUE
            strokeWidth = 4f
            style = Paint.Style.STROKE
            isAntiAlias = true
        }
        
        private val lineFastPaint = Paint().apply {
            // color = Color.parseColor("#00FF00")  // Pure green
            // color = Color.parseColor("#ADFF2F")  // Lemon green (GreenYellow)
            color = Color.parseColor("#228B22")  // Forest green
            // color = Color.parseColor("#32CD32")  // Lime green
            strokeWidth = 5f
            style = Paint.Style.STROKE
            isAntiAlias = true
        }
        
        private val lineSlowPaint = Paint().apply {
            color = Color.RED
            strokeWidth = 5f
            style = Paint.Style.STROKE
            isAntiAlias = true
        }
        
        private val targetPaint = Paint().apply {
            color = Color.parseColor("#228B22")  // Forest green
            strokeWidth = 3f
            style = Paint.Style.STROKE
            isAntiAlias = true
        }
        
        private val thresholdPaint = Paint().apply {
            color = Color.parseColor("#FFA500")  // Orange
            strokeWidth = 2f
            style = Paint.Style.STROKE
            isAntiAlias = true
            pathEffect = android.graphics.DashPathEffect(floatArrayOf(15f, 15f), 0f)
        }
        
        private val bandPaint = Paint().apply {
            color = Color.parseColor("#33FFA500")  // Transparent orange
            style = Paint.Style.FILL
        }
        
        private val textPaint = Paint().apply {
            color = Color.BLACK
            textSize = 40f
            isAntiAlias = true
        }
        
        private val titlePaint = Paint().apply {
            color = Color.parseColor("#228B22")
            textSize = 56f
            isAntiAlias = true
            isFakeBoldText = true
        }
        
        private val failureTitlePaint = Paint().apply {
            color = Color.RED
            textSize = 56f
            isAntiAlias = true
            isFakeBoldText = true
        }
        
        private val timePaint = Paint().apply {
            color = Color.parseColor("#228B22")
            textSize = 44f
            isAntiAlias = true
            isFakeBoldText = true
        }
        
        private val timePaintFailure = Paint().apply {
            color = Color.RED
            textSize = 44f
            isAntiAlias = true
            isFakeBoldText = true
        }
        
        private val percentPaintPositive = Paint().apply {
            color = Color.parseColor("#228B22")  // Green for faster (positive)
            textSize = 72f
            isAntiAlias = true
            isFakeBoldText = true
        }
        
        private val percentPaintNegative = Paint().apply {
            color = Color.RED  // Red for slower (negative)
            textSize = 72f
            isAntiAlias = true
            isFakeBoldText = true
        }
        
        private val failureOverlayPaint = Paint().apply {
            color = Color.argb(64, 255, 0, 0)  // 25% opacity red
            style = Paint.Style.FILL
        }
        
        private val failureTextPaint = Paint().apply {
            color = Color.RED
            textSize = 48f
            isAntiAlias = true
            isFakeBoldText = true
            textAlign = Paint.Align.CENTER
        }
        
        private val gridPaint = Paint().apply {
            color = Color.LTGRAY
            strokeWidth = 1f
            style = Paint.Style.STROKE
        }
        
        private val hintPaint = Paint().apply {
            color = Color.GRAY
            textSize = 28f
            isAntiAlias = true
            textAlign = Paint.Align.RIGHT
        }
        
        private val backgroundPaint = Paint().apply {
            color = Color.WHITE
            style = Paint.Style.FILL
        }
        
        private val targetLabelPaint = Paint().apply {
            color = Color.parseColor("#228B22")  // Same green as target line
            textSize = 32f
            isAntiAlias = true
            textAlign = Paint.Align.RIGHT
        }
        
        private val thresholdLabelPaint = Paint().apply {
            color = Color.parseColor("#FFA500")  // Same orange as threshold lines
            textSize = 32f
            isAntiAlias = true
            textAlign = Paint.Align.LEFT
        }
        
        override fun onDraw(canvas: Canvas) {
            super.onDraw(canvas)
            
            // Draw white background
            canvas.drawRect(0f, 0f, width.toFloat(), height.toFloat(), backgroundPaint)
            
            if (times.isEmpty() || velocities.isEmpty()) return
            
            val padding = 100f
            val leftPadding = 130f  // More space for left labels
            val topPadding = 160f  // Extra space for title
            val bottomPadding = 60f  // Space for hint text
            val chartWidth = width - leftPadding - padding
            val chartHeight = height - topPadding - padding - bottomPadding
            
            // Determine success/failure
            val isSuccess = status == "VALID"
            
            // Calculate time deviation percentage
            // Positive = faster (shorter time), Negative = slower (longer time)
            val timeDeviationPercent = if (targetTime > 0) {
                (targetTime - detectedTime) / targetTime * 100.0
            } else 0.0
            
            // Draw title and info based on status
            if (isSuccess) {
                canvas.drawText("PACE ACHIEVED!", leftPadding, 55f, titlePaint)
            } else {
                val failureType = if (status == "TOO_FAST") "TOO FAST" else "TOO SLOW"
                canvas.drawText(failureType, leftPadding, 55f, failureTitlePaint)
            }
            
            // Draw time info: "Time: 9.12s (Target: 10.00s)" - two decimal places
            val timeInfoPaint = if (isSuccess) timePaint else timePaintFailure
            val timeStr = String.format("Time: %.2fs  (Target: %.2fs)", detectedTime, targetTime)
            canvas.drawText(timeStr, leftPadding, 115f, timeInfoPaint)
            
            // Draw percentage deviation (right side of header)
            val percentStr = String.format("%+.1f%%", timeDeviationPercent)
            val percentPaint = if (timeDeviationPercent >= 0) percentPaintPositive else percentPaintNegative
            val percentWidth = percentPaint.measureText(percentStr)
            canvas.drawText(percentStr, width - padding - percentWidth, 100f, percentPaint)
            
            // Calculate scales
            val tMax = times.maxOrNull() ?: 1.0
            val vMin = (vSlow * 0.85).coerceAtMost(velocities.minOrNull() ?: 0.0)
            val vMax = (vFast * 1.15).coerceAtLeast(velocities.maxOrNull() ?: 2.0)
            val vRange = vMax - vMin
            
            fun xPos(t: Double) = leftPadding + (t / tMax * chartWidth).toFloat()
            fun yPos(v: Double) = topPadding + chartHeight - ((v - vMin) / vRange * chartHeight).toFloat()
            
            // Draw velocity tunnel band (between FAST and SLOW)
            val bandPath = Path()
            bandPath.moveTo(xPos(0.0), yPos(vFast))
            bandPath.lineTo(xPos(tMax), yPos(vFast))
            bandPath.lineTo(xPos(tMax), yPos(vSlow))
            bandPath.lineTo(xPos(0.0), yPos(vSlow))
            bandPath.close()
            canvas.drawPath(bandPath, bandPaint)
            
            // Draw grid
            for (i in 0..4) {
                val y = topPadding + chartHeight * i / 4
                canvas.drawLine(leftPadding, y, leftPadding + chartWidth, y, gridPaint)
            }
            for (i in 0..5) {
                val x = leftPadding + chartWidth * i / 5
                canvas.drawLine(x, topPadding, x, topPadding + chartHeight, gridPaint)
            }
            
            // Draw target line (green)
            canvas.drawLine(xPos(0.0), yPos(vTarget), xPos(tMax), yPos(vTarget), targetPaint)
            
            // Draw FAST and SLOW threshold lines (orange dashed)
            canvas.drawLine(xPos(0.0), yPos(vFast), xPos(tMax), yPos(vFast), thresholdPaint)
            canvas.drawLine(xPos(0.0), yPos(vSlow), xPos(tMax), yPos(vSlow), thresholdPaint)
            
            // Draw velocity line with color changes based on thresholds
            for (i in 1 until times.size) {
                val v1 = velocities[i - 1]
                val v2 = velocities[i]
                val avgV = (v1 + v2) / 2
                
                val segmentPaint = when {
                    avgV > vFast -> lineFastPaint    // Above FAST threshold -> lemon green
                    avgV < vSlow -> lineSlowPaint    // Below SLOW threshold -> red
                    else -> lineNormalPaint          // In tunnel -> blue
                }
                
                canvas.drawLine(xPos(times[i-1]), yPos(v1), xPos(times[i]), yPos(v2), segmentPaint)
            }
            
            // Draw axis labels
            textPaint.textSize = 36f
            textPaint.color = Color.BLACK
            canvas.drawText("Time (s)", leftPadding + chartWidth / 2 - 60, height - 20f, textPaint)
            canvas.drawText("0", leftPadding - 15, topPadding + chartHeight + 40, textPaint)
            canvas.drawText(String.format("%.1f", tMax), leftPadding + chartWidth - 40, topPadding + chartHeight + 40, textPaint)
            
            // Left side velocity labels
            // Target velocity in green
            canvas.drawText(String.format("%.2f", vTarget), leftPadding - 10, yPos(vTarget) + 10, targetLabelPaint)
            
            // Right side labels: FAST and SLOW in orange
            canvas.drawText("FAST", leftPadding + chartWidth + 15, yPos(vFast) + 10, thresholdLabelPaint)
            canvas.drawText("SLOW", leftPadding + chartWidth + 15, yPos(vSlow) + 10, thresholdLabelPaint)
            
            // Draw failure overlay if not successful
            if (!isSuccess) {
                // Semi-transparent overlay on chart area
                canvas.drawRect(leftPadding, topPadding, leftPadding + chartWidth, topPadding + chartHeight, failureOverlayPaint)
                
                // Failure message in center of chart
                val centerX = leftPadding + chartWidth / 2
                val centerY = topPadding + chartHeight / 2
                canvas.drawText("Repeat swim", centerX, centerY, failureTextPaint)
            }
            
            // Draw hint at bottom right
            canvas.drawText("Tap anywhere to close", width - padding, height - 5f, hintPaint)
        }
    }
}
