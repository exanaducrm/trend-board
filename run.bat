@echo off
chcp 65001 > nul
cd /d "%~dp0"
setlocal enabledelayedexpansion
title 인기 보드

echo.
echo   인기 보드를 시작합니다.
echo.

rem ---------------------------------------------------------------- 파일 확인
set "MISSING="
for %%F in (server.py collectors.py dashboard.html) do (
  if not exist "%%F" set "MISSING=!MISSING! %%F"
)
if defined MISSING (
  echo   [문제] 이 폴더에 다음 파일이 없습니다.
  echo    !MISSING!
  echo.
  echo   run.bat 과 같은 폴더에 모두 있어야 합니다.
  echo   현재 폴더: %CD%
  echo.
  pause
  exit /b 1
)

rem ---------------------------------------------------------------- 파이썬 찾기
call :find_python
if defined PY goto have_python

echo   이 PC에 파이썬이 없습니다.
echo.
echo   공식 배포처에서 내려받아 이 사용자 계정에만 설치합니다.
echo   약 30MB 내려받고 1~2분 걸립니다. 관리자 권한은 필요 없습니다.
echo.
choice /c YN /n /m "   설치할까요?  설치 Y / 직접 설치 N : "
if errorlevel 2 goto manual_install
echo.

rem 1순위: 윈도우 기본 설치 도구
where winget > nul 2>&1
if not errorlevel 1 (
  echo   winget 으로 설치합니다.
  winget install -e --id Python.Python.3.12 --scope user --silent --accept-source-agreements --accept-package-agreements
  call :find_python
  if defined PY goto installed
  echo   winget 설치가 확인되지 않아 다른 방법으로 시도합니다.
  echo.
)

rem 2순위: python.org 설치 파일
set "INSTALLER=%TEMP%\python-3.12.6-amd64.exe"
echo   python.org 에서 설치 파일을 내려받습니다.
curl -L --fail -o "%INSTALLER%" https://www.python.org/ftp/python/3.12.6/python-3.12.6-amd64.exe
if errorlevel 1 goto download_failed
for %%F in ("%INSTALLER%") do if %%~zF LSS 10000000 goto download_failed

echo   설치 중입니다. 창이 뜨면 끝날 때까지 기다려 주세요.
"%INSTALLER%" /passive InstallAllUsers=0 PrependPath=1 Include_launcher=1 Include_test=0
del "%INSTALLER%" > nul 2>&1

call :find_python
if defined PY goto installed

echo.
echo   [문제] 설치는 했는데 파이썬을 찾지 못합니다.
echo   이 창을 닫고 run.bat 을 다시 실행해 보세요.
echo   그래도 안 되면 PC를 다시 시작한 뒤 실행해 주세요.
echo.
pause
exit /b 1

:installed
echo.
echo   파이썬 설치를 마쳤습니다.
echo.

:have_python
echo   사용할 파이썬
echo     %PY%
"%PY%" --version
echo.

rem ---------------------------------------------------------------- 패키지
rem 표시 파일 대신 실제로 불러와지는지 확인한다.
rem 다른 PC 로 폴더를 복사했을 때 설치를 건너뛰는 일을 막는다.
"%PY%" -c "import fastapi, uvicorn, httpx, bs4" > nul 2>&1
if errorlevel 1 (
  echo   필요한 패키지를 설치합니다. 1~2분 걸립니다.
  echo.
  rem requirements.txt 가 같이 복사되지 않았을 수 있으니 그때는 목록을 직접 지정한다
  if exist "requirements.txt" (
    "%PY%" -m pip install --disable-pip-version-check -r requirements.txt
  ) else (
    echo   requirements.txt 가 없어 필요한 것만 직접 설치합니다.
    "%PY%" -m pip install --disable-pip-version-check fastapi uvicorn httpx beautifulsoup4
  )
  echo.
  "%PY%" -c "import fastapi, uvicorn, httpx, bs4" > nul 2>&1
  if errorlevel 1 (
    echo   [문제] 설치했는데도 패키지를 불러오지 못합니다.
    echo   위에 찍힌 pip 메시지를 먼저 확인해 주세요.
    echo   파이썬이 여러 개 깔려 서로 다른 곳에 설치되었을 수도 있습니다.
    echo.
    echo   직접 확인하시려면 아래를 실행해 보세요.
    echo     "%PY%" -m pip install fastapi uvicorn httpx beautifulsoup4
    echo     "%PY%" -c "import fastapi"
    echo.
    pause
    exit /b 1
  )
  echo   설치를 마쳤습니다.
  echo.
)

rem ---------------------------------------------------------------- 실행
rem 서버가 뜰 시간을 준 뒤 브라우저를 연다
start "" /min cmd /c "timeout /t 3 > nul & start "" http://127.0.0.1:8787"

echo   이 창을 닫으면 보드도 꺼집니다. 끄려면 Ctrl+C 를 누르세요.
echo.

"%PY%" server.py

echo.
if errorlevel 1 (
  echo   서버가 오류로 멈췄습니다. 위에 찍힌 메시지를 확인해 주세요.
) else (
  echo   서버가 종료되었습니다.
)
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
rem PATH 가 아직 갱신되지 않았을 수 있으니 흔한 설치 경로도 뒤진다
for /d %%D in ("%LOCALAPPDATA%\Programs\Python\Python3*") do (
  if exist "%%D\python.exe" (
    set "PY=%%D\python.exe"
    exit /b 0
  )
)
exit /b 1

:download_failed
echo.
echo   [문제] 설치 파일을 내려받지 못했습니다.
echo   회사 네트워크에서 막혔거나 주소가 바뀌었을 수 있습니다.
echo.
:manual_install
echo   직접 설치하시려면 아래 주소에서 내려받으세요.
echo.
echo     https://www.python.org/downloads/
echo.
echo   설치 화면 맨 아래 "Add python.exe to PATH" 를 반드시 체크해야 합니다.
echo   설치가 끝나면 run.bat 을 다시 실행해 주세요.
echo.
start "" https://www.python.org/downloads/
pause
exit /b 1
