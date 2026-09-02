@echo off
chcp 65001 > nul
cd /d "%~dp0"
setlocal enabledelayedexpansion
title 쇼핑 트렌드 점검

if "%~1"=="" (set "TARGET=snx_best") else (set "TARGET=%~1")

echo.
echo   [!TARGET!] 페이지 구조를 분석합니다.
echo.

call :find_python
if not defined PY (
echo   [문제] 파이썬을 찾지 못했습니다.
echo   run.bat 을 먼저 실행하면 파이썬을 설치해 줍니다.
echo.
pause
exit /b 1
)

"%PY%" server.py --inspect !TARGET!

echo.
echo   ----------------------------------------------------------
echo   출력된 내용을 그대로 복사해서 알려주시면 설정을 맞춰 드립니다.
echo.
echo   다른 소스를 보려면 명령 프롬프트에서
echo     python server.py --inspect auction_best
echo     python server.py --preview elevenst_best
echo   처럼 실행하세요.
echo   ----------------------------------------------------------
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
