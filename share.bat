@echo off
chcp 65001 > nul
cd /d "%~dp0"
setlocal enabledelayedexpansion
title 쇼핑 트렌드 (사내망 공유)

echo.
echo   같은 네트워크에 있는 다른 PC에서도 볼 수 있게 띄웁니다.
echo.

call :find_python
if not defined PY (
  echo   [문제] 파이썬을 찾지 못했습니다. run.bat 을 먼저 실행해 주세요.
  echo.
  pause
  exit /b 1
)

echo   이 PC의 주소
for /f "tokens=2 delims=:" %%A in ('ipconfig ^| findstr /c:"IPv4"') do (
  for /f "tokens=* delims= " %%B in ("%%A") do echo     http://%%B:8787
)
echo.
echo   위 주소 중 하나를 다른 PC의 브라우저에 입력하면 됩니다.
echo   접속이 안 되면 윈도우 방화벽에서 파이썬 허용을 눌러 주세요.
echo.
echo   이 창을 닫으면 보드도 꺼집니다.
echo.

start "" /min cmd /c "timeout /t 3 > nul & start "" http://127.0.0.1:8787"
"%PY%" server.py --host 0.0.0.0

echo.
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
