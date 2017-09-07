# Sample script to install Python and pip under Windows
# Authors: Olivier Grisel and Kyle Kastner
# License: CC0 1.0 Universal: http://creativecommons.org/publicdomain/zero/1.0/

$BASE_URL = "https://www.python.org/ftp/python/"
$GET_PIP_URL = "https://bootstrap.pypa.io/get-pip.py"
$GET_PIP_PATH = "C:\get-pip.py"


function DownloadPython ($python_version, $platform_suffix) {
    $webclient = New-Object System.Net.WebClient
    $filename = "python-" + $python_version + $platform_suffix + ".msi"
    $url = $BASE_URL + $python_version + "/" + $filename

    $basedir = $pwd.Path + "\"
    $filepath = $basedir + $filename
    if (Test-Path $filename) {
        Write-Host "Reusing" $filepath
        return $filepath
    }

    # Download and retry up to 5 times in case of network transient errors.
    Write-Host "Downloading" $filename "from" $url
    $retry_attempts = 3
    for($i=0; $i -lt $retry_attempts; $i++){
        try {
            $webclient.DownloadFile($url, $filepath)
            break
        }
        Catch [Exception]{
            Start-Sleep 1
        }
   }
   Write-Host "File saved at" $filepath
   return $filepath
}


function InstallPython ($python_version, $architecture, $python_home) {
    Write-Host "Installing Python" $python_version "for" $architecture "bit architecture to" $python_home
    if (Test-Path $python_home) {
        Write-Host $python_home "already exists, skipping."
        return $false
    }
    if ($architecture -eq "32") {
        $platform_suffix = ""
    } else {
        $platform_suffix = ".amd64"
    }
    $filepath = DownloadPython $python_version $platform_suffix
    Write-Host "Installing" $filepath "to" $python_home
    $args = "/qn /i $filepath TARGETDIR=$python_home"
    Write-Host "msiexec.exe" $args
    Start-Process -FilePath "msiexec.exe" -ArgumentList $args -Wait -Passthru
    Write-Host "Python $python_version ($architecture) installation complete"
    return $true
}


function InstallPip ($python_home) {
    $pip_path = $python_home + "/Scripts/pip.exe"
    $python_path = $python_home + "/python.exe"
    if (-not(Test-Path $pip_path)) {
        Write-Host "Installing pip..."
        $webclient = New-Object System.Net.WebClient
        $webclient.DownloadFile($GET_PIP_URL, $GET_PIP_PATH)
        Write-Host "Executing:" $python_path $GET_PIP_PATH
        Start-Process -FilePath "$python_path" -ArgumentList "$GET_PIP_PATH" -Wait -Passthru
    } else {
        Write-Host "pip already installed."
    }
}

function InstallPackage ($python_home, $pkg) {
    $pip_path = $python_home + "/Scripts/pip.exe"
    & $pip_path install $pkg
}

function InstallMissingHeaders () {
    # Visual Studio 2008 is missing stdint.h, but you can just download one
    # from the web.
    # http://stackoverflow.com/questions/126279/c99-stdint-h-header-and-ms-visual-studio
    $webclient = New-Object System.Net.WebClient

    $include_dirs = @("C:\Program Files\Microsoft SDKs\Windows\v7.0\Include",
                      "C:\Program Files\Microsoft SDKs\Windows\v7.1\Include",
                      "C:\Program Files (x86)\Microsoft Visual Studio 9.0\VC\include",
                      "C:\Program Files (x86)\Microsoft Visual Studio 10.0\VC\include",
                      "C:\Users\appveyor\AppData\Local\Programs\Common\Microsoft\Visual C++ for Python\9.0\VC\include")

    Foreach ($include_dir in $include_dirs) {
    $urls = @(@("https://raw.githubusercontent.com/chemeris/msinttypes/master/stdint.h", "stdint.h"),
             @("https://raw.githubusercontent.com/chemeris/msinttypes/master/inttypes.h", "inttypes.h"))

    Foreach ($i in $urls) {
        $url = $i[0]
        $filename = $i[1]

        $filepath = "$include_dir\$filename"
        if (Test-Path $filepath) {
            Write-Host $filename "already exists in" $include_dir
            continue
        }

        Write-Host "Downloading remedial " $filename " from" $url "to" $filepath
        $retry_attempts = 2
        for($i=0; $i -lt $retry_attempts; $i++){
            try {
                $webclient.DownloadFile($url, $filepath)
                break
            }
            Catch [Exception]{
                Start-Sleep 1
            }
       }

       if (Test-Path $filepath) {
           Write-Host "File saved at" $filepath
       } else {
           # Retry once to get the error message if any at the last try
           $webclient.DownloadFile($url, $filepath)
       }
    }
    }
}

function main () {
    InstallPython $env:PYTHON_VERSION $env:PYTHON_ARCH $env:PYTHON
    InstallPip $env:PYTHON
    InstallPackage $env:PYTHON wheel
    InstallMissingHeaders
}

main
