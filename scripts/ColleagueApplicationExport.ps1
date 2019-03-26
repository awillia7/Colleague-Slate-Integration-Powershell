# Load values from .env and setup global variables
#region

Get-Content ($PSScriptRoot + "/../.env") | ForEach-Object {
    $var = $_.Split("=", 2)
    if ($var[1]) {
        New-Variable -Name $var[0] -Value $var[1]
    }
}

$SFTP_SOURCE_PATH = $PSScriptRoot + "/../sftp/"
$SFTP_DESTINATION_PATH = "/incoming/colleague/applications/"
$SFTP_SECURE_PASSWORD = $SFTP_PASSWORD | ConvertTo-SecureString -AsPlainText -Force

#endregion

# Database Queries
#region

function Get-SQLData($sql, $source, $database) {
    $connectionString = "Data Source=$source; " +
        "Integrated Security=SSPI; " +
        "Initial Catalog=$database"

    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection
    $SqlConnection.ConnectionString = $connectionString
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.CommandText = $sql
    $SqlCmd.Connection = $SqlConnection
    $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
    $SqlAdapter.SelectCommand = $SqlCmd
    $dataset = New-Object System.Data.DataSet
    $SqlAdapter.Fill($dataset)
    $SqlConnection.Close()

    return $dataset
}

function Get-ColleagueApplicationData {
    $sql = @"
    WITH stpr AS (
    SELECT LEFT(STUDENT_PROGRAMS_ID, 7) AS STUDENTS_ID
    , RIGHT(STUDENT_PROGRAMS_ID, LEN(STUDENT_PROGRAMS_ID) - 8) AS ACADEMIC_PROGRAM
    , STPR_START_DATE
    , RANK() OVER (PARTITION BY LEFT(STUDENT_PROGRAMS_ID, 7) ORDER BY STPR_START_DATE DESC, RIGHT(STUDENT_PROGRAMS_ID, LEN(STUDENT_PROGRAMS_ID) - 8)) AS [RANK]
    FROM STPR_DATES
    INNER JOIN APP_REC_ORGS AS aro ON aro.APPLICANTS_ID = LEFT(STUDENT_PROGRAMS_ID, 7)
        AND APP_REC_ORG_IDS = 'SLATE'
    WHERE STPR_DATES.POS = 1
    AND STPR_START_DATE <= GETDATE()
    AND (STPR_END_DATE > GETDATE() OR STPR_END_DATE IS NULL)
    )
    SELECT a.APPL_CRM_APPLICATION_NO AS [CrmApplicationId]
    , aro.APP_REC_CRM_IDS AS [CrmPersonId]
    , a.APPL_DATE AS [ImportDate]
    , a.APPL_APPLICANT AS [ErpId]
    , stpr.ACADEMIC_PROGRAM AS [StudentProgram]
    FROM APPLICATIONS AS a
    INNER JOIN APP_REC_ORGS AS aro ON aro.APPLICANTS_ID = a.APPL_APPLICANT
        AND APP_REC_ORG_IDS = 'SLATE'
    LEFT OUTER JOIN stpr ON stpr.STUDENTS_ID = a.APPL_APPLICANT AND stpr.[RANK] = 1
    WHERE APPL_START_TERM IN ($STUDENT_PROGRAM_TERMS)
    AND a.APPL_CRM_APPLICATION_NO IS NOT NULL
"@

    return Get-SQLData -sql $sql -source $COLLEAGUE_SQL_SOURCE -database $COLLEAGUE_SQL_DATABASE
}

#endregion

# SFTP to Slate
#region

function Invoke-SFTPToSlate($sftp_filter) {
    # Must install in administrator mode
    # Install-Module -Name Posh-SSH
    $credentials = New-Object -TypeName System.Management.Automation.PSCredential `
        -ArgumentList $SFTP_USERNAME,$SFTP_SECURE_PASSWORD
    
    $session = New-SFTPSession -ComputerName $SFTP_HOST -Credential $credentials -AcceptKey
    
    #Upload the files to the SFTP path
    $files = Get-ChildItem  -Path $SFTP_SOURCE_PATH -Filter $sftp_filter
    foreach ($file in $files) {
        $file = "../sftp/" + $file
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

# Script
#region

$dataset = Get-ColleagueApplicationData

if ($dataset.Tables[0].Rows.Count -gt 0)
{
    $path = $SFTP_SOURCE_PATH + "CollToSlate_$(Get-Date -f yyyy-MM-dd_HH_mm_ss).csv"
    $dataset.Tables[0] | Export-Csv -Path $path -NoTypeInformation
    #Invoke-SFTPToSlate "CollToSlate*.csv"
}

#endregion