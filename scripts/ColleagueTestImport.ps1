# Load values from .env and setup global variables
#region

Get-Content ($PSScriptRoot + "/../.env") | ForEach-Object {
    $var = $_.Split("=", 2)
    if ($var[1]) {
        New-Variable -Name $var[0] -Value $var[1]
    }
}

$SFTP_SOURCE_PATH = $PSScriptRoot + "/../sftp/"
$SFTP_DESTINATION_PATH = "/incoming/colleague/tests/"
# Read SFTP_USERNAME, SFTP_PASSWORD, and SFTP_FLAG from .env
$SFTP_SECURE_PASSWORD = $SFTP_PASSWORD | ConvertTo-SecureString -AsPlainText -Force

$JSON_DATA = $PSScriptRoot + "/../test_scores/data.json"

$ColleagueCredentials = @{
    UserId=$COLLEAGUE_USERID
    Password=$COLLEAGUE_PASSWORD
} | ConvertTo-Json

$SlateCredentials = @{
    UserId=$SLATE_USERID
    Password=$SLATE_PASSWORD
}

#Get a new Colleague API Token
function Get-CollApiToken($Uri, $Credentials){
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls
    return Invoke-RestMethod -Method POST `
    -Uri "$Uri/session/login" -Body $Credentials `
    -ContentType "application/json"
}

#create the Colleague API Header with the provided token
function Get-CollApiHeader($Token) {
    return @{"X-CustomCredentials"=$Token}
}

#create the Slate API Header with provided username and password
function Get-SlateApiHeader($Credentials) {
    $pair = $Credentials."UserId" + ":" + $Credentials."Password"
    $bytes = [System.Text.Encoding]::ASCII.GetBytes($pair)
    $base64 = [System.Convert]::ToBase64String($bytes)
    $basicAuthValue = "Basic $base64"
    return @{"Authorization"=$basicAuthValue}
}

#endregion

# Slate Web API Calls
#region

function Get-TestScores($Uri, $Credentials) {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    $Header = Get-SlateApiHeader $Credentials

    try {
        $result = Invoke-RestMethod -Method Get `
        -Uri "$Uri" `
        -Headers $Header `
        -ContentType "application/json"
    } catch {
        $result = $null
    }

    return $result
}

#endregion

# Colleague Web API Calls
#region

function Import-TestScore($Uri, $Credentials, $data) {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls
    try {
        $Token = Get-CollApiToken $Uri $Credentials
        $Header = Get-CollApiHeader $Token

        $result = Invoke-RestMethod -Method Post `
        -Uri "$Uri/recruiter-test-scores" `
        -Body $data `
        -Headers $Header `
        -ContentType "application/json"
    } catch {
        $result = $null
    }

    return $result
}

#endregion

# File Processing
#region

function Get-TestScoreInJson($ts) 
{
    $oldTestScores = Get-Content -Raw -Path $JSON_DATA | ConvertFrom-Json | Select-Object -ExpandProperty TestScores

    foreach ($oldTestScore in $oldTestScores) 
    {
        if ($oldTestScore.TestId -eq $ts.TestId -and $oldTestScore.SubtestType -eq $ts.SubtestType) 
        {
            return $true
        }
    }
    
    return $false
}

function Add-TestRecord($ts) {
    $testData = @{
        TestId = $ts.TestId
        ErpId = $ts.ErpProspectId
        ImportDate = Get-Date -Format d
        SubtestType = $ts.SubtestType
        #ELFBatch = $elfBatch
        #Error = $error
    }
    
    $oldApps = Get-Content -Raw -Path $JSON_DATA | ConvertFrom-Json
    $oldApps.TestScores += $testData
    $oldApps | ConvertTo-Json | Out-File -FilePath $JSON_DATA 
}

function Add-SFTPFiles() {
    $sftp_tests_data = Get-Content $JSON_DATA | ConvertFrom-Json
    $csv_tests = @()
    $new_info = $false
    
    foreach ($test in $sftp_tests_data.TestScores)
    {
        if ($null -ne $test.ErpId)
        {
            $new_info = $true
            $csv_tests += $test
        }
    }

    if ($new_info)
    {
        # SFTP csv file
        $path = $SFTP_SOURCE_PATH + "CollTestToSlate_$(Get-Date -f yyyy-MM-dd_HH_mm_ss).csv"
        $csv_tests | Export-Csv -Path $path -NoTypeInformation
    }
}

#endregion

# SFTP to Slate
#region

function Invoke-SFTPToSlate() {
    # Must install in administrator mode
    # Install-Module -Name Posh-SSH
    $credentials = New-Object -TypeName System.Management.Automation.PSCredential `
        -ArgumentList $SFTP_USERNAME,$SFTP_SECURE_PASSWORD
    
    $session = New-SFTPSession -ComputerName $SFTP_HOST -Credential $credentials -AcceptKey
    
    #Upload the files to the SFTP path
    $files = Get-ChildItem  -Path $SFTP_SOURCE_PATH -Filter "CollTestToSlate*.csv"
    foreach ($file in $files) {
        $file = $PSScriptRoot + "/../sftp/" + $file
        Set-SFTPFile -SessionId $session.SessionId -LocalFile $file -RemotePath $SFTP_DESTINATION_PATH
        Remove-Item -Path $file
    }

    #Disconnect SFTP session
    if ($session = Get-SFTPSession -SessionId $session.SessionId) {
        $session.Disconnect()
    }
    $null = Remove-SFTPSession -SFTPSession $session
}

#endregion

# Main
#region

$testScores = Get-TestScores $SLATE_TEST_SCORES_API_URI $SlateCredentials
#$lastTest = $null
#$scoreImported = $false

foreach ($score in $testScores.row)
{
    # FTP Test imported
    #if ($lastTest -and $lastTest -ne $score.TestId -and $scoreImported) {
        # Add code to sftp import date
        #$scoreImported = $false
        #if ($SFTP_FLAG -eq 1) {
            #Add-SFTPFiles
            #Invoke-SFTPToSlate
        #}
    #}
    #$lastTest = $score.TestId

    # Need to check if application already processed
    $need_to_import = -Not (Get-TestScoreInJson($score))
    
    if ($need_to_import) {
        
        # Import Application
        #$scoreImported = $true
        $data = $score | ConvertTo-Json
        $importResponse = Import-TestScore $COLLEAGUE_API_URI $ColleagueCredentials $data

        # Record imported file
        if ($null -ne $importResponse) {
            Add-TestRecord $score
        }
    } 
}

#if ($SFTP_FLAG -eq 1 -and $scoreImported) {
    #Add-SFTPFiles
    #Invoke-SFTPToSlate
#}

#endregion