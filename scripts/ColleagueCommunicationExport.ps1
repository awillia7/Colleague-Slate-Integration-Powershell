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

function Get-ColleagueCommunication() {
    # Query communication information
    $year = $FA_FILE_SUITE.Substring($FA_FILE_SUITE.length - 2, 2)
    $codes = "'FAC" + $year + "RAL', 'FAC" + $year + "AL', 'FAC" + $year + "ISR'"
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
WHERE MAILING_CORR_RECEIVED IN ($codes)
  AND v.VAL_ACTION_CODE_1 IN (1, 2)
UNION
SELECT aro.APP_REC_CRM_IDS AS [SLATE_ID]
  , 'FAC${year}ISR' AS [MAILING_CORR_RECEIVED]
  , ISIR_RESULTS.IRES_RECEIPT_DT AS [MAILING_CORR_RECVD_ASGN_DT]
  , 'Received' AS [MAILING_CORR_RECVD_STATUS]
FROM CS_${FA_FILE_SUITE}
  INNER JOIN ISIR_RESULTS ON CS_${FA_FILE_SUITE}.CS_FED_ISIR_ID = ISIR_RESULTS.ISIR_RESULTS_ID
  INNER JOIN APP_REC_ORGS AS aro ON aro.APPLICANTS_ID = CS_${FA_FILE_SUITE}.CS_STUDENT_ID
    AND APP_REC_ORG_IDS = 'SLATE'
WHERE ISIR_RESULTS.IRES_RECEIPT_DT IS NOT NULL
  AND aro.APPLICANTS_ID NOT IN (SELECT CH_CORR.MAILING_ID FROM CH_CORR WHERE CH_CORR.MAILING_CORR_RECEIVED = 'FAC${year}ISR');
"@

    return Get-SQLData -sql $sql -source $COLLEAGUE_SQL_SOURCE -database $COLLEAGUE_SQL_DATABASE
}

function Get-ColleagueMPNData() {
    # Query MPN status
    $sql = @"
    WITH last_direct_loan AS (
    SELECT DLIA_STUDENT_ID AS ID
    ,  MAX(DLIA_AWARD_CREATE_DATE) AS LAST_LOAN_DATE
    FROM DLI_AWARD AS direct_loan
    INNER JOIN APP_REC_ORGS AS aro ON aro.APPLICANTS_ID = direct_loan.DLIA_STUDENT_ID
        AND APP_REC_ORG_IDS = 'SLATE'
    WHERE DLIA_AWARD_YEAR = '$MPN_YEAR'
    GROUP BY DLIA_STUDENT_ID
    )
    SELECT DISTINCT aro.APP_REC_CRM_IDS AS [SLATE_ID]
    , 'trad_mpn' AS [MAILING_CORR_RECEIVED]
    , last_direct_loan.LAST_LOAN_DATE AS [MAILING_CORR_RECVD_ASGN_DT]
    , CASE
        WHEN DLIA_MPN_STATUS = 'A' THEN 'Complete'
        WHEN DLIA_MPN_STATUS = 'R' THEN 'Incomplete'
        ELSE 'Incomplete'
    END AS [MAILING_CORR_RECVD_STATUS]
    FROM DLI_AWARD AS direct_loan
    INNER JOIN last_direct_loan ON last_direct_loan.ID = direct_loan.DLIA_STUDENT_ID
        AND last_direct_loan.LAST_LOAN_DATE = direct_loan.DLIA_AWARD_CREATE_DATE
    INNER JOIN APP_REC_ORGS AS aro ON aro.APPLICANTS_ID = direct_loan.DLIA_STUDENT_ID
        AND APP_REC_ORG_IDS = 'SLATE'
    INNER JOIN APPLICATIONS AS a ON direct_loan.DLIA_STUDENT_ID = a.APPL_APPLICANT
        AND a.APPL_START_TERM IN ('$TRAD_APP_TERM')
    WHERE DLIA_AWARD_YEAR = '$MPN_YEAR';
"@

    return Get-SQLData -sql $sql -source $COLLEAGUE_SQL_SOURCE -database $COLLEAGUE_SQL_DATABASE
}

function Get-ColleagueEntranceCounseling {
    # Query MPN status
    $sql = @"
    WITH interview_date AS (
    SELECT FAIN_STUDENT_ID
    , MAX(ISNULL(FA_INTERVIEW_ADDDATE, FAIN_ENTRANCE_COMMENT_DATE)) AS INTERVIEW_DATE
    FROM FA_INTERVIEW
    WHERE FAIN_LOAN_CODE = 'SUB'
      AND FAIN_STUDENT_ID IS NOT NULL
    GROUP BY FAIN_STUDENT_ID
    )
    SELECT aro.APP_REC_CRM_IDS AS [SLATE_ID]
    , 'trad_counseling' AS [MAILING_CORR_RECEIVED]
    , interview_date.INTERVIEW_DATE [MAILING_CORR_RECVD_ASGN_DT]
    , CONVERT(NVARCHAR, interview_date.INTERVIEW_DATE, 101) AS [MAILING_CORR_RECVD_STATUS]
    FROM interview_date
    INNER JOIN APP_REC_ORGS AS aro ON aro.APPLICANTS_ID = interview_date.FAIN_STUDENT_ID
        AND APP_REC_ORG_IDS = 'SLATE'
    INNER JOIN APPLICATIONS AS a ON interview_date.FAIN_STUDENT_ID = a.APPL_APPLICANT
        AND a.APPL_START_TERM IN ('$TRAD_APP_TERM');
"@

    return Get-SQLData -sql $sql -source $COLLEAGUE_SQL_SOURCE -database $COLLEAGUE_SQL_DATABASE
}

function Get-ColleagueAwardStatus {
    # Query Award Status
    $ta_suite = 'TA_{0}' -f $FA_FILE_SUITE
    $ta_id = '{0}_ID' -f $ta_suite
    $sql = @"
    WITH award_status AS (
    SELECT LEFT($ta_id, 7) AS TA_PERSON_ID
    , MAX(TA_TERM_ACTION_DATE) AS TERM_ACTION_DATE
    , MAX(TA_TERM_ACTION) AS TERM_ACTION
    FROM $ta_suite
    WHERE ($ta_id LIKE '%*FDLU*%'
    OR $ta_id LIKE '%*FDLU2*%'
    OR $ta_id LIKE '%*FDLU3*%'
    OR $ta_id LIKE '%*FDLU4*%'
    OR $ta_id LIKE '%*FDLU5*%'
    OR $ta_id LIKE '%*FDLU6*%'
    OR $ta_id LIKE '%*FDLS*%'
    OR $ta_id LIKE '%*FDLS2*%'
    OR $ta_id LIKE '%*FDLS3*%'
    OR $ta_id LIKE '%*FDLS4*%'
    OR $ta_id LIKE '%*FDLS5*%')
    GROUP BY LEFT($ta_id, 7)
    )
    SELECT aro.APP_REC_CRM_IDS AS [SLATE_ID]
    , 'trad_awardstatus' AS [MAILING_CORR_RECEIVED]
    , TERM_ACTION_DATE AS [MAILING_CORR_RECVD_ASGN_DT] 
    , CASE
        WHEN award_status.TERM_ACTION = 'O' THEN 'Incomplete'
        WHEN award_status.TERM_ACTION = 'R' THEN 'Rejected'
        WHEN award_status.TERM_ACTION = 'C' THEN 'Rejected'
        WHEN award_status.TERM_ACTION = 'S' THEN 'Accepted'
        WHEN award_status.TERM_ACTION = 'A' THEN 'Accepted'
    END AS [MAILING_CORR_RECVD_STATUS]
    FROM award_status
    INNER JOIN APP_REC_ORGS AS aro ON aro.APPLICANTS_ID = award_status.TA_PERSON_ID
        AND APP_REC_ORG_IDS = 'SLATE'
    INNER JOIN APPLICATIONS AS a ON award_status.TA_PERSON_ID = a.APPL_APPLICANT
        AND a.APPL_START_TERM IN ('$TRAD_APP_TERM')
        AND award_status.TERM_ACTION <> 'O';
"@

    return Get-SQLData -sql $sql -source $COLLEAGUE_SQL_SOURCE -database $COLLEAGUE_SQL_DATABASE
}

# Get Username and Initial Passwords
function Get-ColleagueAuthenication {
    #Query username info
    $sql = @"
    SELECT aro.APP_REC_CRM_IDS AS [SLATE_ID]
    , 'trad_login' AS [MAILING_CORR_RECEIVED]
    , ORG_ENTITY_ENV_ADDDATE AS [MAILING_CORR_RECVD_ASGN_DT]
    , OEE_USERNAME AS [MAILING_CORR_RECVD_STATUS]
    FROM ORG_ENTITY_ENV
    INNER JOIN APP_REC_ORGS AS aro ON aro.APPLICANTS_ID = OEE_RESOURCE
        AND APP_REC_ORG_IDS = 'SLATE'
    INNER JOIN APPLICATIONS AS a ON OEE_RESOURCE = a.APPL_APPLICANT
        AND a.APPL_START_TERM IN ('$TRAD_APP_TERM');
"@
    $auth_dataset = Get-SQLData -sql $sql -source $COLLEAGUE_SQL_SOURCE -database $COLLEAGUE_SQL_DATABASE

    # Loop through usernames and get password info from CROA
    $pw_dataset = $null
    foreach ($row in $auth_dataset.Tables[0].Rows) {
        $slate_id = $row.SLATE_ID
        $username = $row.MAILING_CORR_RECVD_STATUS
        $auth_date = $row.MAILING_CORR_RECVD_ASGN_DT
        $sql = @"
        SELECT '$slate_id' AS [SLATE_ID]
        , 'trad_password' AS [MAILING_CORR_RECEIVED]
        , CONVERT(DATE, '$auth_date') AS [MAILING_CORR_RECVD_ASGN_DT]
        , INITIAL_PASSWORD AS [MAILING_CORR_RECVD_STATUS]
        FROM X_AD_CRED
        WHERE PERSON_PIN_USER_ID = '$username';
"@

        if ($pw_dataset) {
            $pw = Get-SQLData -sql $sql -source $ODS_SQL_SOURCE -database $ODS_SQL_DATABASE
            $pw_dataset.Tables[0].Merge($pw.Tables[0])
        } else {
            $pw_dataset = Get-SQLData -sql $sql -source $ODS_SQL_SOURCE -database $ODS_SQL_DATABASE
        }
    }

    if ($pw_dataset) {
        $auth_dataset.Tables[0].Merge($pw_dataset.Tables[0])
    }

    return $auth_dataset
}

# Retrieve needed non-course information and push over as interaction
function Get-ColleagueNonCourseData {
    # Query for HSFL non-course records
    $sql = @"
    SELECT aro.APP_REC_CRM_IDS AS [SLATE_ID]
    , 'mvnu_hsfl' AS [MAILING_CORR_RECEIVED]
    , STNC_START_DATE AS [MAILING_CORR_RECVD_ASGN_DT]
    , 'Completed' AS [MAILING_CORR_RECVD_STATUS]
    FROM STUDENT_NON_COURSES AS snc
    INNER JOIN APP_REC_ORGS AS aro ON aro.APPLICANTS_ID = snc.STNC_PERSON_ID
        AND APP_REC_ORG_IDS = 'SLATE'
    WHERE snc.STNC_NON_COURSE = 'HSFL'
    AND snc.STNC_SCORE = 2
"@

    return Get-SQLData -sql $sql -source $COLLEAGUE_SQL_SOURCE -database $COLLEAGUE_SQL_DATABASE
}

function Get-ColleagueAthleticAwardsData {
    $sql = @"
    SELECT aro.APP_REC_CRM_IDS AS [SLATE_ID]
    , fa_list.SA_AWARD AS [MAILING_CORR_RECEIVED]
    , fa_list.SA_DATE AS [MAILING_CORR_RECVD_ASGN_DT]
    , fa_list.SA_AMOUNT AS [MAILING_CORR_RECVD_STATUS]
    FROM ${AWARD_TABLE}_AWARD_LIST AS fa_list
    INNER JOIN AWARDS ON fa_list.SA_AWARD = AWARDS.AW_ID
    INNER JOIN APP_REC_ORGS AS aro ON aro.APPLICANTS_ID = fa_list.SA_STUDENT_ID
        AND APP_REC_ORG_IDS = 'SLATE'
    WHERE SA_AWARD IN (${ATHLETE_AWARDS})
    AND SA_ACTION = 'A'
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

$dataset = Get-ColleagueCommunication

$mpn_data = Get-ColleagueMPNData
$dataset.Tables[0].Merge($mpn_data.Tables[0])

$entrance_counseling = Get-ColleagueEntranceCounseling
$dataset.Tables[0].Merge($entrance_counseling.Tables[0])

$award_status = Get-ColleagueAwardStatus
$dataset.Tables[0].Merge($award_status.Tables[0])

$colleague_auth = Get-ColleagueAuthenication
$dataset.Tables[0].Merge($colleague_auth.Tables[0])

$noncourse_data = Get-ColleagueNonCourseData
$dataset.Tables[0].Merge($noncourse_data.Tables[0])

$athlete_award_data = Get-ColleagueAthleticAwardsData
$dataset.Tables[0].Merge($athlete_award_data.Tables[0])

if ($dataset.Tables[0].Rows.Count -gt 0)
{
    $path = $SFTP_SOURCE_PATH + "CollCommToSlate_$(Get-Date -f yyyy-MM-dd_HH_mm_ss).csv"
    $dataset.Tables[0] | Export-Csv -Path $path -NoTypeInformation
    Invoke-SFTPToSlate "CollCommToSlate*.csv"
}

#endregion