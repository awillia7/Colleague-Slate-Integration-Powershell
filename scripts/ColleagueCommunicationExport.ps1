# Load values from .env and setup global variables
#region

Get-Content ($PSScriptRoot + "/../.env") | ForEach-Object {
    $var = $_.Split("=", 2)
    if ($var[1]) {
        New-Variable -Name $var[0] -Value $var[1]
    }
}

$SFTP_SOURCE_PATH = $PSScriptRoot + "/../sftp/"
$SFTP_DESTINATION_PATH = "/incoming/colleague/communication/"
$SFTP_SECURE_PASSWORD = $SFTP_PASSWORD | ConvertTo-SecureString -AsPlainText -Force

#endregion

# Colleague Database Calls
#region

function Get-ColleagueCommunication()
{
    $connectionString = "Data Source=$COLLEAGUE_SQL_SOURCE; " +
        "Integrated Security=SSPI; " +
        "Initial Catalog=$COLLEAGUE_SQL_DATABASE"

    # Query communication information
    $sql = @"
SELECT aro.APP_REC_CRM_IDS AS [SLATE_ID]
  , MAILING_CORR_RECEIVED
  , MAILING_CORR_RECVD_ASGN_DT
  , CASE
      WHEN v.VAL_ACTION_CODE_1 = 1 THEN 'Received'
	  ELSE v.VAL_EXTERNAL_REPRESENTATION
  END AS [MAILING_CORR_RECVD_STATUS]
FROM CH_CORR
  INNER JOIN APP_REC_ORGS AS aro ON aro.APPLICANTS_ID = CH_CORR.MAILING_ID
    AND APP_REC_ORG_IDS = 'SLATE'
  INNER JOIN VALS AS v ON v.VAL_INTERNAL_CODE = CH_CORR.MAILING_CORR_RECVD_STATUS
    AND v.VALCODE_ID = 'CORR.STATUSES'
WHERE MAILING_CORR_RECEIVED IN ('FAC19RAL', 'FAC19AL', 'FAC19ISR')
  AND v.VAL_ACTION_CODE_1 IN (1, 2);
"@

    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection
    $SqlConnection.ConnectionString = $connectionString
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.CommandText = $sql
    $SqlCmd.Connection = $SqlConnection
    $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
    $SqlAdapter.SelectCommand = $SqlCmd
    $DataSet = New-Object System.Data.DataSet
    $SqlAdapter.Fill($DataSet)
    $SqlConnection.Close()
    #$data = $dataset.Tables[0]
    
    return $dataset
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

$dataset = Get-ColleagueCommunication
if ($dataset.Tables[0].Rows.Count -gt 0)
{
    $path = $SFTP_SOURCE_PATH + "CollCommToSlate_$(Get-Date -f yyyy-MM-dd_HH_mm_ss).csv"
    $dataset.Tables[0] | Export-Csv -Path $path -NoTypeInformation
    Invoke-SFTPToSlate "CollCommToSlate*.csv"
}

#endregion