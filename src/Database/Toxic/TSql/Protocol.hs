module Database.Toxic.TSql.Protocol where

import qualified Data.ByteString as BS
import Data.Word
import qualified Data.Vector as V

data RowDescriptionField = RowDescriptionField {
  rowDescriptionFieldName :: BS.ByteString,
  rowDescriptionFieldOid :: Word32,
  rowDescriptionFieldAttributeNumber :: Word16,
  rowdescriptionFieldDataType :: Word32,
  rowDescriptionFieldSize :: Word16,
  rowDescriptionFieldModifier :: Word32,
  rowDescriptionFieldFormatCode :: Word16
  } deriving (Eq, Show)

-- | http://www.postgresql.org/docs/9.2/static/protocol-message-formats.html
data Message =
    AuthenticationOk
  | AuthenticationKerberosV5
  | AuthenticationCleartextPassword
  | AuthenticationMD5Password { authenticationMd5Salt :: Word32 }
  | AuthenticationSCMCredential
  | AuthenticationGSS
  | AuthenticationSSPI
  | AuthenticationGSSContinue {
    authenticationGssContinueLength :: Word32,
    authenticationGssContinueBytes :: BS.ByteString
    }
  | BackendKeyData {
    backendKeyDataProcessId :: Word32,
    backendKeyDataSecretKey :: Word32
    }
  | Bind {
    bindLength :: Word32,
    bindDestinationPortal :: String,
    bindSourceStatement :: String,
    bindParameterFormatCodes :: V.Vector Word16,
    bindParameters :: V.Vector BS.ByteString
    }
  | BindComplete
  | CancelRequest {
    cancelRequestPid :: Word32,
    cancelSecretKey :: Word32
    }
  | Close {
    closeChooseStatementOrPortal :: Word8,
    closeName :: BS.ByteString
    }
  | CloseComplete
  | CommandComplete {
    commandCompleteTag :: BS.ByteString
    }
  | CopyData {
    copyDataData :: BS.ByteString
    }
  | CopyDone
  | CopyFail {
    copyFailMessage :: BS.ByteString
    }
  | CopyInResponse {
    copyInResponseFormat :: Word8,
    copyInResponseFormatCodes :: V.Vector Word16
    }
  | CopyOutResponse {
    copyOutResponseFormat :: Word8,
    copyOutResponseFormatCodes :: V.Vector Word16
    }
  | CopyBothResponse {
    copyBothResponseFormat :: Word8,
    copyBothResponseColumns :: V.Vector Word16
    }
  | DataRow {
    dataRowValues :: V.Vector BS.ByteString
    }
  | Describe {
    describeChooseStatementOrPortal :: Word8,
    describeName :: BS.ByteString
    }
  | EmptyQueryResponse
  | ErrorResponse {
    errorResponseTypes :: V.Vector Word8,
    errorResponseValues :: V.Vector BS.ByteString
    }
  | Execute {
    executeName :: BS.ByteString,
    executeMaxNumRows :: Word32
    }
  | Flush
  | FunctionCall {
    functionCallOid :: Word32,
    functionCallFormatCodes :: V.Vector Word16,
    functionCallArguments :: V.Vector BS.ByteString,
    functionCallResultFormatCode :: Word8
    }
  | FunctionCallResponse {
    functionCallResponseValue :: Maybe BS.ByteString
    }
  | NoData
  | NoticeResponse {
    noticeResponseFieldTypes :: V.Vector Word8,
    noticeResponseFieldValues :: V.Vector BS.ByteString
    }
  | NotificationResponse {
    notificationResponsePid :: Word32,
    notificationResponseChannel :: BS.ByteString,
    notificationResponsePayload :: BS.ByteString
    }
  | ParameterDescription {
    parameterDescriptionOids :: V.Vector Word32
    }
  | ParameterStatus {
    parameterStatusName :: BS.ByteString,
    parameterStatusValue :: BS.ByteString
    }
  | Parse {
    parseDestination :: BS.ByteString,
    parseQuery :: BS.ByteString,
    parseParameterTypes :: V.Vector Word32
    }
  | ParseComplete
  | PasswordMessage {
    passwordMessagePassword :: BS.ByteString
    }
  | PortalSuspended
  | Query {
    queryQuery :: BS.ByteString
    }
  | ReadyForQuery {
    readyForQueryStatus :: Word8
    }
  | RowDescription {
    rowDescriptionFields :: V.Vector RowDescriptionField
    }
  | SSLRequest
  | StartupMessage {
    startupMessageParameters :: V.Vector (BS.ByteString, BS.ByteString)
    }
  | Sync
  | Terminate
  deriving (Eq, Show)
