namespace CUBRID.Data.CUBRIDClient
{
  internal enum MsgId
  {
    NotImplemented,
    NotSupported,
    DefineWhetherTraceInformationIsLogged,
    InvalidLOBPosition,
    TheConnectionIsNotOpen,
    TheConnectionPropertyHasNotBeenSet,
    DataReaderIsAlreadyOpen,
    InvalidQueryType,
    NotAllParametersAreBound,
    NotAllowedToChangeConnectionStringWhenStateIs,
    InvalidTimeoutValue,
    NotAllowedToChangeLockTimeoutWhenStateIs,
    NotAllowedToChangeConnectionTimeoutWhenStateIs,
    CantChangeDatabase,
    NotSupportedInCUBRID,
    ConnectionAlreadyOpen,
    ErrorWrittingLOBContent,
    ErrorReadingLOBContent,
    NotAValidLOBType,
    InvalidConnectionParameter,
    InvalidConnectionPort,
    InvalidConnectionString,
    DBNameIsEmpty,
    ServerIsEmpty,
    UserIsEmpty,
    PasswordIsEmpty,
    InvalidPropertyName,
    GetBytesCanBeCalledOnlyOnBinaryColumns,
    BufferIndexMustBeValidIndexInBuffer,
    BufferNotLargeEnoughToHoldRequestedData,
    DataOffsetMustBeValidPositionInField,
    ResultsetIsClosed,
    InvalidAttemptToReadDataWhenReaderNotOpen,
    InvalidBufferPosition,
    DbTypeCantBeMappedToCUBRIDDataType,
    CUBRIDDataTypeCantBeMappedToDbType,
    DontKnowHowToWriteParameter,
    ParameterNotFound,
    OnlyCUBRIDParameterObjectsAreValid,
    ParametersMustBeNamed,
    ParameterNameMustStartWith,
    ParameterAlreadyAdded,
    ArgumentMustBeCUBRIDParameter,
    UnknownIsolationLevelNotSupported,
    ConnectionMustBeValidAndOpenToCommit,
    ConnectionMustBeValidAndOpenToRollBack,
    TransactionAlreadyCommittedOrNotPending,
    IncorrectNumberOfFilters,
    NoFiltersSpecified,
    ParameterNotFoundMissingPrefix,
    collectionNameIsNull,
    ConnectionStringIsNULL,
    ConnectFailed,
  }

  internal class Messages
  {
    internal static string GetStrFromId(MsgId id)
    {
      return id.ToString();
    }
  }
}