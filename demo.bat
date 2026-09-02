@echo off
chcp 65001 > nul
cd /d "%~dp0"
setlocal enabledelayedexpansion
title 인기 보드 미리보기

echo.
echo   예시 데이터로 화면만 띄웁니다. 쇼핑몰에 접속하지 않습니다.
echo.

call :find_python
if not defined PY (
echo   [문제] 파이썬을 찾지 못했습니다.
echo   run.bat 을 먼저 실행하면 파이썬을 설치해 줍니다.
echo.
pause
exit /b 1
)

start "" /min cmd /c "timeout /t 3 > nul & start "" http://127.0.0.1:8787"
"%PY%" server.py --demo
pause
exit /b 0

rem ---------------------------------------------------------------- 보조
:find_python
set "PY="
for %%C in (python py) do (
  %%C --version > nul 2>&1
  if not errorlevel 1 (
    set "PY=%%C"
    exit /b 0
  )
)
for /d %%D in ("%LOCALAPPDATA%\Programs\Python\Python3*") do (
  if exist "%%D\python.exe" (
    set "PY=%%D\python.exe"
    exit /b 0
  )
)
exit /b 1
