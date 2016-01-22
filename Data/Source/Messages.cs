using System;
using System.Collections.Generic;
using System.Text;

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
		TransactionAlreadyCommittedOrNotPending,
		IncorrectNumberOfFilters,
		NoFiltersSpecified,
		ParameterNotFoundMissingPrefix,
        collectionNameIsNull,
	}

	internal class Messages
	{
		internal static string GetStrFromId(MsgId id)
		{
			return id.ToString();
		}
	}
}
