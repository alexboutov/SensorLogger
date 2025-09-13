package com.example.sensorlogger

import android.content.Context
import android.content.Intent
import android.os.Bundle
import android.view.inputmethod.EditorInfo
import android.view.inputmethod.InputMethodManager
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

        // Restore last saved azimuth
        val last = prefs.getString(PREF_AZIMUTH, "0.0") ?: "0.0"
        binding.azimuthEditText.setText(last)

        // Initial label for the single button
        updateStartStopLabel()

        // Save + hide keyboard when user taps "Done"
        binding.azimuthEditText.setOnEditorActionListener { v, actionId, _ ->
            if (actionId == EditorInfo.IME_ACTION_DONE) {
                saveAzimuth()
                v.clearFocus()
                hideKeyboard()
                true
            } else {
                false
            }
        }

        // Also hide keyboard when focus leaves the field
        binding.azimuthEditText.setOnFocusChangeListener { _, hasFocus ->
            if (!hasFocus) hideKeyboard()
        }

        // Single START/STOP toggle
        binding.startStopButton.setOnClickListener {
            // Always save and hide keyboard on button press
            saveAzimuth()
            hideKeyboard()
            binding.azimuthEditText.clearFocus()

            if (!isLogging) {
                // START: normalize + pass to service
                val azText = binding.azimuthEditText.text?.toString()?.trim().orEmpty()
                val azimuthDeg = azText.toDoubleOrNull()?.let { v ->
                    // normalize into [0, 360)
                    ((v % 360.0) + 360.0) % 360.0
                } ?: 0.0

                val intent = Intent(this, SensorLoggerService::class.java).apply {
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
        saveAzimuth()
    }

    private fun saveAzimuth() {
        val azText = binding.azimuthEditText.text?.toString()?.trim().orEmpty()
        prefs.edit().putString(PREF_AZIMUTH, azText).apply()
    }

    private fun updateStartStopLabel() {
        binding.startStopButton.text = if (isLogging) "STOP" else "START"
    }

    private fun hideKeyboard() {
        val imm = getSystemService(Context.INPUT_METHOD_SERVICE) as InputMethodManager
        val view = currentFocus ?: binding.azimuthEditText
        imm.hideSoftInputFromWindow(view.windowToken, 0)
    }
}
