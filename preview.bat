@echo off
chcp 65001 > nul
cd /d "%~dp0"
setlocal enabledelayedexpansion
title 인기 보드 - 정적 배포 미리보기

echo.
echo   깃허브 페이지에 올렸을 때의 모습을 미리 봅니다.
echo   한 번 수집해 snapshot.json 을 만든 뒤, 그 파일만으로 화면을 띄웁니다.
echo.

call :find_python
if not defined PY (
  echo   [문제] 파이썬을 찾지 못했습니다. run.bat 을 먼저 실행해 주세요.
  echo.
  pause
  exit /b 1
)

"%PY%" server.py --export snapshot.json
if errorlevel 1 (
  echo   [문제] 수집에 실패했습니다. 위 메시지를 확인해 주세요.
  echo.
  pause
  exit /b 1
)

echo   미리보기 주소  http://127.0.0.1:8000/dashboard.html
echo   이 창을 닫으면 미리보기도 꺼집니다.
echo.

start "" /min cmd /c "timeout /t 2 > nul & start "" http://127.0.0.1:8000/dashboard.html"
"%PY%" -m http.server 8000 --bind 127.0.0.1

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
