# Load values from .env and setup global variables
#region

Get-Content ($PSScriptRoot + "/../.env") | ForEach-Object {
    $var = $_.Split("=", 2)
    if ($var[1]) {
        New-Variable -Name $var[0] -Value $var[1]
    }
}

#$SFTP_SOURCE_PATH = $PSScriptRoot + "/../sftp/"
#$SFTP_DESTINATION_PATH = "/incoming/colleague/"
#$SFTP_HOST = "ft.technolutions.net"
#$SFTP_SECURE_PASSWORD = $SFTP_PASSWORD | ConvertTo-SecureString -AsPlainText -Force

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
    return Invoke-RestMethod -Method Get `
    -Uri "$Uri" `
    -Headers $Header `
    -ContentType "application/json"
}

#endregion

# Colleague Web API Calls
#region

function Import-TestScore($Uri, $Credentials, $data) {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls
    $Token = Get-CollApiToken $Uri $Credentials
    $Header = Get-CollApiHeader $Token

    return Invoke-RestMethod -Method Post `
    -Uri "$Uri/recruiter-test-scores" `
    -Body $data `
    -Headers $Header `
    -ContentType "application/json"
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

#endregion

# Main
#region

$testScores = Get-TestScores $SLATE_TEST_SCORES_API_URI $SlateCredentials

foreach ($score in $testScores.row)
{
    # Need to check if application already processed
    $need_to_import = Get-TestScoreInJson($score)

    if (-Not $need_to_import) {

        # Import Application
        $data = $score | ConvertTo-Json
        #$errorFlag = 0
        #$importResponse = Import-TestScore $COLLEAGUE_API_URI $ColleagueCredentials $data
        Import-TestScore $COLLEAGUE_API_URI $ColleagueCredentials $data

        # Record imported file
        Add-TestRecord $score
    } 
}

#if ($SFTP_FLAG -eq 1) {
    #Add-SFTPFiles
    #Invoke-SFTPToSlate
#}

#endregion