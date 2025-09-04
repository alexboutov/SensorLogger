adb shell ls /sdcard/Android/data/com.example.sensorlogger/files/sensor_logs
adb pull /sdcard/Android/data/com.example.sensorlogger/files/sensor_logs ../.
@echo off
setlocal ENABLEDELAYEDEXPANSION

REM === Settings ===
set "PKG=com.example.sensorlogger"
set "REMOTE_DIR=/sdcard/Android/data/%PKG%/files/sensor_logs"

REM === 1) Pull remote CSVs into the current directory (no nested folder) ===
for /f "delims=" %%F in ('adb shell ls -1 %REMOTE_DIR% 2^>nul') do (
  echo %%F | findstr /r /c:"^run_.*\.csv$" >nul
  if not errorlevel 1 (
    adb pull "%REMOTE_DIR%/%%F" . >nul
  )
)

REM === 2) Pick newest by filename (lexicographic = chronological for your pattern) ===
set "newest="
for /f "delims=" %%F in ('dir /b /a-d run_*.csv ^| sort') do (
  set "newest=%%F"
)

if not defined newest (
  echo No run_*.csv files found in %cd%.
  exit /b 1
)

echo === NEWEST: %cd%\%newest%
echo.

REM === 3) TOP 10 ===
echo --- TOP 10 ---
powershell -NoProfile -Command ^
  "Get-Content -Path '%newest%' -TotalCount 10 | ForEach-Object { $_ }"
echo.

REM Count lines once for middle calc
for /f "usebackq tokens=1" %%N in (`powershell -NoProfile -Command "(Get-Content -Path '%newest%').Count"`) do set COUNT=%%N
if not defined COUNT set COUNT=0

REM Choose middle window of 10 lines
set /a midStart = COUNT/2 - 5
if %midStart% lss 0 set midStart=0

REM Clamp end (PowerShell slice will clamp, but we compute end for clarity)
set /a midEnd = midStart + 9

REM === 4) MIDDLE 10 ===
echo --- MIDDLE 10 ---
powershell -NoProfile -Command ^
  "$c=Get-Content -Path '%newest%';" ^
  "$s=[math]::Max(0,%midStart%);" ^
  "$e=[math]::Min($s+9,$c.Count-1);" ^
  "if ($c.Count -gt 0) { $c[$s..$e] | ForEach-Object { $_ } }"
echo.

REM === 5) BOTTOM 20 ===
echo --- BOTTOM 20 ---
powershell -NoProfile -Command ^
  "Get-Content -Path '%newest%' | Select-Object -Last 20 | ForEach-Object { $_ }"
echo.

endlocal
