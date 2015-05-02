module Database.Toxic.TSql.Protocol where

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Char
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
data AuthenticationOk = AuthenticationOk deriving (Eq, Show)
data AuthenticationKerberosV5 = AuthenticationKerberosV5 deriving (Eq, Show)
data AuthenticationCleartextPassword = AuthenticationCleartextPassword deriving (Eq, Show)
data AuthenticationMD5Password = AuthenticationMD5Password {
  authenticationMd5Salt :: Word32
  } deriving (Eq, Show)
data AuthenticationSCMCredential = AuthenticationSCMCredential deriving (Eq, Show)
data AuthenticationGSS = AuthenticationGSS deriving (Eq, Show)
data AuthenticationSSPI = AuthenticationSSPI deriving (Eq, Show)
data AuthenticationGSSContinue = AuthenticationGSSContinue {
  authenticationGssContinueLength :: Word32,
  authenticationGssContinueBytes :: BS.ByteString
  } deriving (Eq, Show)
data BackendKeyData = BackendKeyData {
  backendKeyDataProcessId :: Word32,
  backendKeyDataSecretKey :: Word32
  } deriving (Eq, Show)
data Bind = Bind {
  bindLength :: Word32,
  bindDestinationPortal :: String,
  bindSourceStatement :: String,
  bindParameterFormatCodes :: V.Vector Word16,
  bindParameters :: V.Vector BS.ByteString
  } deriving (Eq, Show)
data BindComplete = BindComplete deriving (Eq, Show)
data CancelRequest = CancelRequest {
  cancelRequestPid :: Word32,
  cancelSecretKey :: Word32
  } deriving (Eq, Show)
data Close = Close {
  closeChooseStatementOrPortal :: Word8,
  closeName :: BS.ByteString
  } deriving (Eq, Show)
data CloseComplete = CloseComplete deriving (Eq, Show)
data CommandComplete = CommandComplete {
  commandCompleteTag :: BS.ByteString
  } deriving (Eq, Show)
data CopyData = CopyData {
  copyDataData :: BS.ByteString
  } deriving (Eq, Show)
data CopyDone = CopyDone deriving (Eq, Show)
data CopyFail = CopyFail {
  copyFailMessage :: BS.ByteString
  } deriving (Eq, Show)
data CopyInResponse = CopyInResponse {
  copyInResponseFormat :: Word8,
  copyInResponseFormatCodes :: V.Vector Word16
  } deriving (Eq, Show)
data CopyOutResponse = CopyOutResponse {
  copyOutResponseFormat :: Word8,
  copyOutResponseFormatCodes :: V.Vector Word16
  } deriving (Eq, Show)
data CopyBothResponse = CopyBothResponse {
  copyBothResponseFormat :: Word8,
  copyBothResponseColumns :: V.Vector Word16
  } deriving (Eq, Show)
data DataRow = DataRow {
  dataRowValues :: V.Vector BS.ByteString
  } deriving (Eq, Show)
data Describe = Describe {
  describeChooseStatementOrPortal :: Word8,
  describeName :: BS.ByteString
  } deriving (Eq, Show)
data EmptyQueryResponse = EmptyQueryResponse deriving (Eq, Show)
data ErrorResponse = ErrorResponse {
  errorResponseTypes :: V.Vector Word8,
  errorResponseValues :: V.Vector BS.ByteString
  } deriving (Eq, Show)
data Execute = Execute {
  executeName :: BS.ByteString,
  executeMaxNumRows :: Word32
  } deriving (Eq, Show)
data Flush = Flush deriving (Eq, Show)
data FunctionCall = FunctionCall {
  functionCallOid :: Word32,
  functionCallFormatCodes :: V.Vector Word16,
  functionCallArguments :: V.Vector BS.ByteString,
  functionCallResultFormatCode :: Word8
  } deriving (Eq, Show)
data FunctionCallResponse = FunctionCallResponse {
  functionCallResponseValue :: Maybe BS.ByteString
  } deriving (Eq, Show)
data NoData = NoData deriving (Eq, Show)
data NoticeResponse = NoticeResponse {
  noticeResponseFieldTypes :: V.Vector Word8,
  noticeResponseFieldValues :: V.Vector BS.ByteString
  } deriving (Eq, Show)
data NotificationResponse = NotificationResponse {
  notificationResponsePid :: Word32,
  notificationResponseChannel :: BS.ByteString,
  notificationResponsePayload :: BS.ByteString
  } deriving (Eq, Show)
data ParameterDescription = ParameterDescription {
  parameterDescriptionOids :: V.Vector Word32
  } deriving (Eq, Show)
data ParameterStatus = ParameterStatus {
  parameterStatusName :: BS.ByteString,
  parameterStatusValue :: BS.ByteString
  } deriving (Eq, Show)
data Parse = Parse {
  parseDestination :: BS.ByteString,
  parseQuery :: BS.ByteString,
  parseParameterTypes :: V.Vector Word32
  } deriving (Eq, Show)
data ParseComplete = ParseComplete deriving (Eq, Show)
data PasswordMessage = PasswordMessage {
  passwordMessagePassword :: BS.ByteString
  } deriving (Eq, Show)
data PortalSuspended = PortalSuspended deriving (Eq, Show)
data Query = Query {
  queryQuery :: BS.ByteString
  } deriving (Eq, Show)

instance Binary Query where
  get = do
    tag <- getWord8
    size <- getWord32be
    bs <- getByteString $ fromIntegral size - fromIntegral (2 * sizeOfWord32) + 1
    null <- getWord8
    return $ Query { queryQuery = bs } 
  put Query { queryQuery = bs } = do
    putWord8 $ fromIntegral $ ord 'Q'
    putWord32be $ 2 * sizeOfWord32 + (fromIntegral $ BS.length bs) - 1
    putByteString bs
    putWord8 0
             
data ReadyForQuery = ReadyForQuery {
  readyForQueryStatus :: Word8
  } deriving (Eq, Show)
data RowDescription = RowDescription {
  rowDescriptionFields :: V.Vector RowDescriptionField
  } deriving (Eq, Show)
data SSLRequest = SSLRequest deriving (Eq, Show)

-- | Sent at the start of a psql session
data StartupMessage = StartupMessage {
  startupMessageProtocolVersion :: Word32,
  startupMessageParameters :: V.Vector (BS.ByteString, BS.ByteString)
  } deriving (Eq, Show)

isNullByte :: Get Bool
isNullByte = do
  next <- lookAhead getWord8
  if next == 0
    then getWord8 >> return True
    else return False

defaultProtocolMajorVersion = 3 :: Word16
defaultProtocolMinorVersion = 0 :: Word16
defaultProtocolVersion =
  (fromIntegral defaultProtocolMajorVersion) `shiftL` 16 +
  (fromIntegral defaultProtocolMinorVersion) :: Word32

instance Binary StartupMessage where
  get = do
    size <- getWord32be
    isolate (fromIntegral $ size - sizeOfWord32) $ do
      protocolVersion <- getWord32be
      let readOneParameter = do
            name <- getLazyByteStringNul
            value <- getLazyByteStringNul
            return (BSL.toStrict name, BSL.toStrict value)
          readParameters = do
            isAtEnd <- isNullByte
            if isAtEnd
              then return []
              else do
                chunk <- readOneParameter
                chunks <- readParameters
                return $ chunk : chunks
      parameters <- readParameters
      return StartupMessage {
        startupMessageProtocolVersion = protocolVersion,
        startupMessageParameters = V.fromList parameters
        }
  put (StartupMessage {
          startupMessageProtocolVersion = protocolVersion,
          startupMessageParameters = parameters }) = do
    let putParameter :: (BS.ByteString, BS.ByteString)  -> Put
        putParameter (name, value) = do
          putByteString name
          putWord8 0
          putByteString value
          putWord8 0
        putParameters = V.mapM_ putParameter parameters
        parametersBs = runPut putParameters
        size = 2 * sizeOfWord32 + (fromIntegral $ BSL.length parametersBs) + 1
    putWord32be size
    putWord32be protocolVersion
    putLazyByteString parametersBs
    putWord8 0
             
data Sync = Sync deriving (Eq, Show)
data Terminate = Terminate deriving (Eq, Show)

sizeOfWord32 :: Word32
sizeOfWord32 = 4


