# Authorization
#region

$sftp_source_path = $PSScriptRoot + "/../sftp/"
$sftp_destination_path = "/incoming/colleague/"
$sftp_host = "ft.technolutions.net"
$sftp_username = "sftpsa@apply.mvnu.edu"
$sftp_password = "2a6349d64ed14f8bb0352b019e4fd552" | ConvertTo-SecureString -AsPlainText -Force

$ColleagueCredentials = @{
    UserId='recint'
    Password='m61907733'
} | ConvertTo-Json

$ColleagueRootUri = "https://qaswebapi.mvnu.edu:8184/ColleagueApi"

$SlateCredentials = @{
    UserId='recint'
    Password='m61907733'
}

$SlateUri = "https://apply.mvnu.edu/manage/query/run?id=9aa20837-a0cd-44a4-a717-d46c8234993a&h=8c553c0a-0744-1f42-75f0-e6212a035194&cmd=service&output=json"

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

function Get-Application($Uri, $Credentials) {
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

function Import-Application($Uri, $Credentials, $data) {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls
    $Token = Get-CollApiToken $Uri $Credentials
    $Header = Get-CollApiHeader $Token

    return Invoke-RestMethod -Method Post `
    -Uri "$Uri/crm-applications" `
    -Body $data `
    -Headers $Header `
    -ContentType "application/json"
}

#endregion

# Colleague Database Calls
#region

$sql_source = 'qascolldb1'
$sql_database = 'coll18_test_portal'
function Get-DatabaseID($app, $person)
{
    $connectionString = "Data Source=$sql_source; " +
        "Integrated Security=SSPI; " +
        "Initial Catalog=$sql_database"

    $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    $connection.Open()

    # Query for ID on applicant record
    $sql = "SELECT APPLICANTS_ID FROM APP_REC_ORGS WHERE APP_REC_ORG_IDS = 'SLATE' AND APP_REC_CRM_IDS = '$person';"
    $command = New-Object System.Data.SqlClient.SqlCommand($sql, $connection)
    $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataset) | Out-Null
    if ($dataset.Tables[0].Rows.Count -ne 1)
    {
        $connection.Close()
        return $null;
    }
    $applicant_id = ($dataset.Tables[0].Rows[0])[0]

    # Query for ID on Application record
    $sql = "SELECT APPL_APPLICANT FROM APPLICATIONS WHERE APPL_REC_ORG_ID = 'SLATE' AND APPL_CRM_APPLICATION_NO = '$app';"

    $command = New-Object System.Data.SqlClient.SqlCommand($sql, $connection)
    $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataset) | Out-Null
    if ($dataset.Tables[0].Rows.Count -ne 1)
    {
        $connection.Close()
        return $null;
    }
    $appl_id = ($dataset.Tables[0].Rows[0])[0]

    $connection.Close()

    if ($appl_id -ne $applicant_id)
    {
        return $null
    }

    return $applicant_id
}
#endregion

# File Processing
#region

$json_data = $PSScriptRoot + "/../applications/data.json"
function Get-ApplicationInJson($app) 
{
    $oldApps = Get-Content -Raw -Path $json_data | ConvertFrom-Json | Select-Object -ExpandProperty Applications

    foreach ($oldApp in $oldApps) 
    {
        if ($oldApp.CrmApplicationId -eq $app) 
        {
            return $true
        }
    }
    
    return $false
}

function Add-SFTPFiles() {
    $sftp_apps_data = Get-Content $json_data | ConvertFrom-Json
    $csv_apps = @()
    $new_info = $false
    
    foreach ($app in $sftp_apps_data.Applications)
    {
        if ($app.ErpId -eq $null)
        {
            #Look up ID
            $erpid = Get-DatabaseID $app.CrmApplicationId $app.CrmPersonId
            
            if ($erpid -ne $null)
            {
                $new_info = $true
                $app.ErpId = $erpid
                $csv_apps += $app
            }
        }
    }

    if ($new_info)
    {
        # SFTP csv file
        $path = $sftp_source_path + "CollToSlate_$(Get-Date -f yyyy-MM-dd_HH_mm_ss).csv"
        $csv_apps | Export-Csv -Path $path -NoTypeInformation
        
        # Update JSON
        $sftp_apps_data | ConvertTo-Json | Out-File -FilePath $json_data 
    }
}

function Add-ApplicationRecord($capp, $cperson, $elfBatch, $error) {
    $appData = @{
        CrmApplicationId = $capp
        CrmPersonId = $cperson
        ELFBatch = $elfBatch
        ImportDate = Get-Date -Format d
        Error = $error
        ErpId = $null
    }

    $oldApps = Get-Content -Raw -Path $json_data | ConvertFrom-Json
    $oldApps.Applications += $appData
    $oldApps | ConvertTo-Json | Out-File -FilePath $json_data 
}
#endregion

# SFTP to Slate
#region

function Invoke-SFTPToSlate() {
    # Must install in administrator mode
    # Install-Module -Name Posh-SSH
    $credentials = New-Object -TypeName System.Management.Automation.PSCredential `
        -ArgumentList $sftp_username,$sftp_password
    
    $session = New-SFTPSession -ComputerName $sftp_host -Credential $credentials -AcceptKey
    
    #Upload the files to the SFTP path
    $files = Get-ChildItem ($sftp_source_path + "/*.csv")
    foreach ($file in $files) {
        #$file = $PSScriptRoot + "/../sftp/" + $file
        Set-SFTPFile -SessionId $session.SessionId -LocalFile $file -RemotePath $sftp_destination_path
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

#$trad_email = "awillia2@mvnu.edu"
$nt_email = "awillia2@mvnu.edu"

$applications = Get-Application $SlateUri $SlateCredentials
foreach ($app in $applications.row)
{
    # Need to check if application already processed
    $need_to_import = -Not (Get-ApplicationInJson $app.CrmApplicationId);

    if ($need_to_import) {
        # Import Application
        $data = $app | ConvertTo-Json
        $errorFlag = 0
        $importResponse = Import-Application $ColleagueRootUri $ColleagueCredentials $data
        
        # Email Errors
        if ($importResponse.ElfErrors) {
            $errors = $importResponse.ElfErrors -replace '~','`n'
            $errorFlag = 1
            
            $body = "Applicant: " + $app.FirstName + " " + $app.LastName + "`nELF Batch: " + $importResponse.ElfBatch + "`nErrors:`n" + $errors
            
            # Send Email with error message
            $email = $nt_email
            Send-MailMessage -To $email -From "no-reply@mvnu.edu" -Subject "Slate Application to Colleage Import Error" -Body $body -SmtpServer "safemx.mvnu.edu"
        }

        # Record imported file
        Add-ApplicationRecord $app.CrmApplicationId $app.CrmPersonId $importResponse.ElfBatch $errorFlag
    } 
}

Add-SFTPFiles
Invoke-SFTPToSlate

#endregion