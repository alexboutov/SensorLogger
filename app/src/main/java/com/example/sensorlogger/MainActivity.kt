package com.example.sensorlogger

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.Build
import android.os.Bundle
import androidx.appcompat.app.AlertDialog
import android.view.inputmethod.EditorInfo
import android.view.inputmethod.InputMethodManager
import androidx.appcompat.app.AppCompatActivity
import com.example.sensorlogger.databinding.ActivityMainBinding

private const val PREFS_NAME = "sensor_logger_prefs"
private const val PREF_AZIMUTH = "azimuth_deg"
private const val PREF_DISTANCE = "distance_meters"
private const val PREF_TARGET_TIME = "target_time_sec"

// Default values
private const val DEFAULT_DISTANCE = "13.716"    // 15 yards in meters
private const val DEFAULT_TARGET_TIME = "10.0"   // seconds

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private val prefs by lazy { getSharedPreferences(PREFS_NAME, MODE_PRIVATE) }

    // Local UI state for the toggle button
    private var isLogging = false
    
    // Receiver for pace feedback
    private val paceResultReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context?, intent: Intent?) {
            if (intent?.action == SensorLoggerService.ACTION_PACE_RESULT) {
                val status = intent.getStringExtra(SensorLoggerService.EXTRA_PACE_STATUS) ?: return
                val targetTime = intent.getDoubleExtra(SensorLoggerService.EXTRA_TARGET_TIME, 0.0)
                val detectedTime = intent.getDoubleExtra(SensorLoggerService.EXTRA_DETECTED_TIME, 0.0)
                
                showPaceFeedback(status, targetTime, detectedTime)
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
        
        prefs.edit()
            .putString(PREF_AZIMUTH, azText)
            .putString(PREF_DISTANCE, distText)
            .putString(PREF_TARGET_TIME, timeText)
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
    
    private fun showPaceFeedback(status: String, targetTime: Double, detectedTime: Double) {
        val title: String
        val message: String
        val icon: Int
        
        when (status) {
            "VALID" -> {
                title = "PACE ACHIEVED"
                message = String.format(
                    "Target: %.1f sec\nActual: %.1f sec\n\nData is valid!",
                    targetTime, detectedTime
                )
                icon = android.R.drawable.ic_dialog_info
            }
            "TOO_FAST" -> {
                title = "TOO FAST"
                message = String.format(
                    "Target: %.1f sec\nActual: %.1f sec\n\nPlease repeat at slower pace.",
                    targetTime, detectedTime
                )
                icon = android.R.drawable.ic_dialog_alert
            }
            else -> { // TOO_SLOW
                title = "TOO SLOW"
                message = String.format(
                    "Target: %.1f sec\nActual: %.1f sec\n\nPlease repeat at faster pace.",
                    targetTime, detectedTime
                )
                icon = android.R.drawable.ic_dialog_alert
            }
        }
        
        AlertDialog.Builder(this)
            .setTitle(title)
            .setMessage(message)
            .setIcon(icon)
            .setPositiveButton("OK", null)
            .show()
    }
}
