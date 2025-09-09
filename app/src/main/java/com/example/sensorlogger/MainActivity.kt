package com.example.sensorlogger

import android.content.Intent
import android.os.Bundle
import android.view.inputmethod.EditorInfo
import androidx.appcompat.app.AppCompatActivity
import com.example.sensorlogger.databinding.ActivityMainBinding

private const val PREFS_NAME = "sensor_logger_prefs"
private const val PREF_AZIMUTH = "azimuth_deg"

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private val prefs by lazy { getSharedPreferences(PREFS_NAME, MODE_PRIVATE) }

    // Local UI state for the toggle button
    private var isLogging = false

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        // 1) Restore last saved azimuth
        val last = prefs.getString(PREF_AZIMUTH, "0.0") ?: "0.0"
        binding.azimuthEditText.setText(last)

        // Initial label for the single button
        updateStartStopLabel()

        // 2) Save when user taps "Done" on the keyboard
        binding.azimuthEditText.setOnEditorActionListener { _, actionId, _ ->
            if (actionId == EditorInfo.IME_ACTION_DONE) {
                saveAzimuth()
                true
            } else false
        }

        // 3) Single START/STOP toggle
        binding.startStopButton.setOnClickListener {
            if (!isLogging) {
                // START: save + normalize + pass to service
                saveAzimuth()
                val azText = binding.azimuthEditText.text?.toString()?.trim().orEmpty()
                val azimuthDeg = azText.toDoubleOrNull()?.let { v ->
                    // normalize into [0, 360)
                    val mod = ((v % 360.0) + 360.0) % 360.0
                    mod
                } ?: 0.0

                val intent = Intent(this, SensorLoggerService::class.java).apply {
                    // Use the service’s constant to avoid key drift
                    putExtra(SensorLoggerService.EXTRA_AZIMUTH_DEG, azimuthDeg)
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
        // 4) Save on lifecycle exit as well
        saveAzimuth()
    }

    private fun saveAzimuth() {
        val azText = binding.azimuthEditText.text?.toString()?.trim().orEmpty()
        prefs.edit().putString(PREF_AZIMUTH, azText).apply()
    }

    private fun updateStartStopLabel() {
        binding.startStopButton.text = if (isLogging) "STOP" else "START"
    }
}
