# Swimming Lap Time Detection Using Inertial Sensors
## A Summary of Algorithms and Methods from Research Literature

### Overview

This document summarizes algorithms for measuring lap time with inertial sensors in swimming, based on papers citing the foundational systematic review by Mooney et al. (2015, PMC4732051).

---

## 1. Foundational Approach: Threshold-Based Detection (Mooney et al. 2015)

**Source:** PMC4732051 - "Inertial Sensor Technology for Elite Swimming Performance Analysis: A Systematic Review"

### Section 4.1.3 Key Findings:

> "The ability to detect wall contact events, and thus record lap times, is paramount, not just from a coaching point of view but also as many other variables are derived from this parameter such as average speed, stroke count, stroke rate and stroke length."

### Algorithm Components:

| Component | Description |
|-----------|-------------|
| **Wall push-off detection** | Rapid increase in acceleration (1g rise over 0.1s duration) |
| **Turn detection** | Zero-crossing algorithm about perpendicular axis as swimmer rotates |
| **Orientation change** | Swimmer switches from vertical to horizontal position |

### Davey et al. (2008) Algorithm Flow:
```
1. Detect swim commencement (vertical→horizontal orientation change)
2. Detect wall push-off: acceleration rise ≥1g (9.81 m/s²) in 0.1s
3. Detect turns: zero-crossing in perpendicular axis
4. Calculate lap time between successive push-off events
```

### Reported Accuracy:
- **Mean difference:** -0.32 ± 0.58s vs video
- **Systematic underestimation** of lap times
- **Problem:** Errors concentrated in first 100m (start detection issues)

### Bächlin & Tröster (2012) Method:
- Low-pass Butterworth filter (0.01 Hz cutoff)
- Push-off: First falling slope in acceleration
- Wall strike: Large impact peak and rising slope
- **Accuracy:** ±0.3s of criterion measure

### Limitations Identified:
1. Arms/legs absorb wall impact (signal attenuation at torso)
2. Individual differences in turning technique
3. False positives from powerful leg kicks
4. Difficulty detecting final touch

---

## 2. Modern Deep Learning Approach (Delhaye et al. 2022)

**Source:** PMC9371205 - "Automatic Swimming Activity Recognition and Lap Time Assessment Based on a Single IMU: A Deep Learning Approach"

### Key Innovation:
Instead of threshold-based detection, use **deep learning classification** to identify swimming phases, then compute lap times from phase transitions.

### Eight Swimming Classes:
1. Wall push (WP)
2. Underwater (UN)
3. Butterfly (BU)
4. Backstroke (BA)
5. Breaststroke (BR)
6. Frontcrawl (FR)
7. Turn (TU)
8. Rest (RS)

### Sensor Setup:
- **Location:** Sacrum (between posterior superior iliac spines)
- **Sensors:** 3D accelerometer (±8g) + 3D gyroscope (±1000°/s)
- **Sample rate:** 280 Hz (downsampled to 50 Hz)
- **Filter:** 2nd order Butterworth low-pass, 10 Hz cutoff

### Neural Network Architecture:
```
Input Layer → 
4x Bidirectional LSTM (128→64→32→32 units) →
Flatten →
Dense (50 units, ReLU) →
Batch Normalization →
Dense (8 units, Softmax) → Output
```

### Key Parameters:
- Window size: 90 frames (1.8 seconds)
- Dropout: 0.25 (LSTM), 0.5 (Dense)
- Learning rate: 0.001 with epoch-dependent decay
- Temporal precision: **0.02 seconds** (20 ms)

### Lap Time Computation from Classifications:

| Lap Type | Start Event | End Event |
|----------|-------------|-----------|
| **START** | Last wall-push prediction | Next-to-last wall-push OR first underwater |
| **MIDDLE** | Last wall-push OR first underwater | Next last turn prediction |
| **END** | Last wall-push OR first underwater | First rest prediction |

### Performance Results:

| Metric | START | MIDDLE | END |
|--------|-------|--------|-----|
| **MAPE** | 1.15% | 1.00% | 4.07% |
| **Bias** | 0.01s | 0.00s | 0.25s |
| **TEM** | 0.39s | 0.37s | 0.98s |

**Overall F1-Score:** 0.96

### Why END has more error:
> "This discrepancy can be explained by the transition between a swimming phase and a rest phase, which sometimes may not be easily identified when the swimmer does not actually touch the wall at the end of a training session."

---

## 3. Macro-Micro Approach (Hamidi Rad et al. 2021)

**Source:** PMC7841373 - "A Novel Macro-Micro Approach for Swimming Analysis"

### Hierarchical Detection:
```
MACRO LEVEL: Activity classification (swimming vs. non-swimming)
    ↓
MICRO LEVEL: Phase detection (wall-push, underwater, swimming, turn)
    ↓
Parameter extraction (lap time, stroke count, velocity)
```

### Sensor Configuration:
- 6 IMUs: shanks, wrists, sacrum, head
- Sample rate: 500 Hz
- **Best single location for lap detection:** Sacrum

### Phase Detection Algorithm:
1. **Madgwick filter** for orientation estimation (fusing accel + gyro + magnetometer)
2. **Quaternion-based** rotation tracking
3. **Machine learning classifier** (Random Forest or SVM) for phase identification

### Accuracy by Sensor Location:

| Location | Lap Detection | Stroke Recognition |
|----------|---------------|-------------------|
| Sacrum | **Highest** | High |
| Head | High | Medium |
| Wrist | Medium | **Highest** |
| Shank | Low | Low |

---

## 4. Tri-axial Accelerometry (Ganzevles et al. 2017)

**Source:** Using Tri-Axial Accelerometry in Daily Elite Swim Training Practice

### Simple Threshold Method:
```python
# Pseudo-code
filter_signal(accel, lowpass=2Hz)
for each window:
    if signal_variance > threshold:
        swimming = True
    if detect_rotation_peak():  # Z-axis perpendicular
        turn_detected = True
        lap_count += 1
```

### Reported Results:
- Lap segmentation: **99% accuracy**
- Stroke recognition: **100% accuracy**
- **Limitation:** Homogeneous population (elite swimmers only)

---

## 5. Commercial Device Algorithms (TritonWear, Garmin, FORM)

### TritonWear:
- MAPE: 3.22% for lap times
- Uses machine learning on IMU data
- Automatic start/finish detection

### Garmin (from Swim.com):
> "Each turn consists of a rapid acceleration off the wall (leg push-off) and a short streamline (pause in arm motion) before the swimmer resumes their stroke, which is the motion pattern trigger."

### FORM Goggles:
- Head-mounted IMU (9-axis: accel + gyro + magnetometer)
- **Turn detection accuracy:** 0.2% error
- Machine learning trained on diverse swimmer populations

---

## 6. Key Algorithm Design Principles

### Signal Processing Pipeline:
```
Raw IMU Data (≥100 Hz)
    ↓
Low-pass Filter (0.5-10 Hz, Butterworth)
    ↓
Orientation Estimation (optional: Madgwick/Kalman)
    ↓
Feature Extraction (variance, peaks, zero-crossings)
    ↓
Classification (threshold OR machine learning)
    ↓
Lap Time = transition_time[n+1] - transition_time[n]
```

### Critical Features for Lap Detection:

| Feature | Use |
|---------|-----|
| **Acceleration magnitude** | Wall push-off detection (≥1g spike) |
| **Z-axis rotation** | Flip turn detection |
| **Signal variance** | Swimming vs. stationary discrimination |
| **Orientation change** | Start/stop detection |

### Recommended Thresholds (from literature):

| Parameter | Threshold | Source |
|-----------|-----------|--------|
| Push-off acceleration | 1.0g (9.81 m/s²) rise in 0.1s | Mooney 2015 |
| Low-pass filter cutoff | 0.5-10 Hz | Various |
| Variance window | 0.5-2.0 seconds | Delhaye 2022 |
| Minimum lap duration | 10-15 seconds (25m pool) | Practical |

---

## 7. Applying to Phone-Based Detection (SensorLogger)

### Challenges for Phone vs. Dedicated IMU:
1. **Placement:** Phone in pocket vs. sacrum-mounted sensor
2. **Sample rate:** Phone ~200 Hz vs. dedicated 280-500 Hz
3. **Sensor quality:** Consumer MEMS vs. research-grade
4. **Orientation:** Phone orientation may shift during activity

### Recommended Adaptations:

#### For Walking Phase Detection (analogous to lap detection):

```kotlin
// Based on research findings
class WalkPhaseDetector {
    // START detection: Find FIRST significant acceleration burst
    // (like detecting push-off, but for walking)
    val pushoffAccelThreshold = 1.5  // m/s² (lower than swimming 1g)
    
    // END detection: Find sustained quiet period
    // (like detecting rest phase after swimming)
    val varianceThreshold = 0.8      // Variance > 0.8 = walking
    val minQuietDuration = 1.0       // seconds of quiet to confirm stop
    
    // Filter: Match swimming research parameters
    val lowpassCutoff = 10.0         // Hz (same as Delhaye 2022)
    val windowSize = 100             // samples at 200Hz = 0.5s
}
```

#### Key Insight from Research:
The **deep learning approach** (Delhaye 2022) achieved much better accuracy than threshold methods because it handles:
- Inter-subject variability (different walking/swimming styles)
- Intra-subject variability (different speeds)
- Noisy transitions (not always clear boundaries)

### Recommendation for SensorLogger:
1. **Short-term:** Improve threshold-based detection using variance patterns
2. **Long-term:** Train a simple classifier (Random Forest or small neural network) on your walking test data

---

## 8. Summary Table: Algorithm Comparison

| Method | Accuracy | Complexity | Real-time? | Reference |
|--------|----------|------------|------------|-----------|
| Threshold (acceleration peak) | ±0.32s | Low | Yes | Davey 2008 |
| Threshold (slope detection) | ±0.3s | Low | Yes | Bächlin 2012 |
| Machine Learning (RF/SVM) | ~99% | Medium | Yes | Hamidi Rad 2021 |
| Deep Learning (Bi-LSTM) | **MAPE 1%** | High | No* | Delhaye 2022 |

*Deep learning requires significant computation but can run on mobile devices with optimization.

---

## References

1. Mooney R, et al. (2015). Inertial Sensor Technology for Elite Swimming Performance Analysis. *Sensors* 16(1):18. PMC4732051

2. Delhaye E, et al. (2022). Automatic Swimming Activity Recognition and Lap Time Assessment Based on a Single IMU. *Sensors* 22(15):5786. PMC9371205

3. Hamidi Rad M, et al. (2021). A Novel Macro-Micro Approach for Swimming Analysis. *Front. Bioeng. Biotechnol.* 8:597738. PMC7841373

4. Ganzevles S, et al. (2017). Using Tri-Axial Accelerometry in Daily Elite Swim Training Practice. *Sensors* 17(5):990.

5. Davey N, et al. (2008). Validation trial of an accelerometer-based sensor platform for swimming. *Sports Technology* 1:202-207.
