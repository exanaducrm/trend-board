@echo off
chcp 65001 > nul
cd /d "%~dp0"
setlocal enabledelayedexpansion
title 인기 보드 - cURL 설정 만들기

if not exist "curl.txt" (
  echo.
  echo   curl.txt 파일이 없습니다.
  echo.
  echo   1. 크롬에서 목록이 있는 페이지를 엽니다
  echo   2. F12 - Network - Fetch/XHR 로 거르고 새로고침
  echo   3. 응답에 상품 목록이 들어 있는 요청을 찾습니다
  echo   4. 우클릭 - Copy - Copy as cURL ^(bash^)
  echo   5. 이 폴더에 curl.txt 로 저장하고 다시 실행
  echo.
  pause
  exit /b 1
)

call :find_python
if not defined PY (
echo   [문제] 파이썬을 찾지 못했습니다.
echo   run.bat 을 먼저 실행하면 파이썬을 설치해 줍니다.
echo.
pause
exit /b 1
)

"%PY%" server.py --from-curl curl.txt

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
