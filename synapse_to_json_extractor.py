# Existing function for extracting position data
def extract_gga_data(self, date, vehicle_id): # position
  """Extract GGA data from Synapse for a specific date and vehicle."""
  self.logger.info(f"Extracting GGA data for vehicle {vehicle_id} on {date}")
  
  date_slashed = date.replace('-', '/')[2:]
  file_path = os.path.join(self.output_dir, f"gga_data_{vehicle_id}_{date}.json")
  
  query = f"""
  SELECT
      CONVERT(VARCHAR, [NestedArray].[MessageSentDateTimeUtc] AT TIME ZONE 'UTC' AT TIME ZONE 'Central Europe Standard Time', 127) AS MessageSentDateTimeString,
      [NestedArray].[MessageType] AS MessageType,
      [NestedArray].[MessageSenderType] AS MessageSenderType,
      [NestedArray].[MessageSenderId] AS MessageSenderId,
      [NestedArray].[MessageSenderGatewayHostId] AS MessageSenderGatewayHostId,
      [NestedArray].[IsMessageSentDelivered] AS IsMessageSentDelivered,
      [NestedArray].[MessageBodyLoaded] AS MessageBodyLoaded,
      [NestedArray].[Latitude] AS Latitude,
      [NestedArray].[Longitude] AS Longitude,
      [NestedArray].[MessageSentDateTimeUtc] AS MessageSentDateTimeUtc
  FROM
      OPENROWSET(
          BULK 'https://seafar(confidential)/v{vehicle_id.zfill(6)}-messages/Renoir/Prod/{date_slashed}/*/*.json.gz',
          FORMAT = 'CSV',
          FIELDQUOTE = '0x0b',
          FIELDTERMINATOR ='0x0b',
          ROWTERMINATOR = '0x0b',
          PARSER_VERSION = '1.0',
          DATA_COMPRESSION = 'GZIP'
      )
      WITH (
          jsonContent varchar(MAX)
      ) AS [result]
  CROSS APPLY OPENJSON(JSON_QUERY([jsonContent], '$'))
  WITH 
  (
      [MessageType] varchar(255) '$.MessageType',
      [MessageSentDateTimeUtc] DATETIME2 '$.MessageSentDateTimeUtc',
      [MessageSenderType] varchar(255) '$.MessageSenderType',
      [MessageSenderId] varchar(255) '$.MessageSenderId',
      [MessageSenderGatewayHostId] varchar(255) '$.MessageSenderGatewayHostId',
      [IsMessageSentDelivered] Bit '$.IsMessageSentDelivered',
      [MessageBodyLoaded] varchar(255) '$.Message.MessageBodyLoaded',
      [Latitude] varchar(255) '$.Message.Latitude',
      [Longitude] varchar(255) '$.Message.Longitude'
  ) AS [NestedArray]
  WHERE [NestedArray].[MessageType] = '$--GGA' and
      [NestedArray].[MessageSentDateTimeUtc] >= '{date}T00:00:00'
  ORDER BY MessageSentDateTimeUtc
  """
  
  self._execute_and_save(query, file_path)

# New function I added to extract fuel consumption data
def extract_pm061_data(self, date, vehicle_id): # fuel consumption
  """Extract PM061 reporting data from Synapse."""
  self.logger.info(f"Extracting PM061 data for vehicle {vehicle_id} on {date}")
  
  date_slashed = date.replace('-', '/')[2:]
  file_path = os.path.join(self.output_dir, f"pm061_data_{vehicle_id}_{date}.json")
  
  query = f"""
      SELECT
          CONVERT(VARCHAR, [NestedArray].[MessageSentDateTimeUtc] AT TIME ZONE 'UTC' AT TIME ZONE 'Central Europe Standard Time', 127) AS MessageSentDateTimeString,
          [NestedArray].[MessageType] AS MessageType,
          [NestedArray].[MessageSentDateTimeUtc] AS MessageSentDateTimeUtc,
          [NestedArray].[MessageSenderType] AS MessageSenderType,
          [NestedArray].[MessageSenderId] AS MessageSenderId,
          [NestedArray].[MessageSenderGatewayHostId] AS MessageSenderGatewayHostId,
          [NestedArray].[MessageBodyLoaded] AS MessageBodyLoaded,
          [NestedArray].[EntityId] AS EntityId,
          [NestedArray].[ValueTypeId] AS ValueTypeId,
          [NestedArray].[Value] AS Value,
          [NestedArray].[ValueUnitId] AS ValueUnitId,
          [NestedArray].[TimeIntervalId] AS TimeIntervalId,
          [NestedArray].[TimeIntervalAggregationTypeId] AS TimeIntervalAggregationTypeId
      FROM
          OPENROWSET(
              BULK 'https://seafar(confidential)/v{vehicle_id.zfill(6)}-messages/Renoir/Prod/{date_slashed}/*/*.json.gz',
              FORMAT = 'CSV',
              FIELDQUOTE = '0x0b',
              FIELDTERMINATOR ='0x0b',
              ROWTERMINATOR = '0x0b',
              PARSER_VERSION = '1.0',
              DATA_COMPRESSION = 'GZIP'
          )
          WITH (
              jsonContent varchar(MAX)
          ) AS [result]
      CROSS APPLY OPENJSON(JSON_QUERY([jsonContent], '$'))
      WITH 
      (
          [MessageType] varchar(255) '$.MessageType',
          [MessageSentDateTimeUtc] DATETIME2 '$.MessageSentDateTimeUtc',
          [MessageSenderType] varchar(255) '$.MessageSenderType',
          [MessageSenderId] varchar(255) '$.MessageSenderId',
          [MessageSenderGatewayHostId] varchar(255) '$.MessageSenderGatewayHostId',
          [MessageBodyLoaded] varchar(255) '$.Message.MessageBodyLoaded',
          [EntityId] varchar(50) '$.Message.EntityId',
          [ValueTypeId] int '$.Message.ValueTypeId',
          [Value] float '$.Message.Value.Value',
          [ValueUnitId] varchar(50) '$.Message.ValueUnitId',
          [TimeIntervalId] INT '$.Message.TimeIntervalId',
          [TimeIntervalAggregationTypeId] INT '$.Message.TimeIntervalAggregationTypeId'
      ) AS [NestedArray]
      WHERE [NestedArray].[MessageType] = '$PM061'
      ORDER BY MessageSentDateTimeUtc
  """
  
  self._execute_and_save(query, file_path)

# Database connection
class SynapseExtractor:
  """Extracts data from Azure Synapse and stores it as JSON files."""
  
  def __init__(self, output_dir="./data", logger=None):
      """Initialize the extractor with connection details and logger."""
      # Setup logger
      if logger is None:
          self.logger =setup_logger('synapse_to_json_extrator')
      else:
          self.logger = logger
          
      # Output directory
      self.output_dir = output_dir
      os.makedirs(output_dir, exist_ok=True)
      
      # Synapse connection details
      self.synapse_conn_string = os.environ.get('SYNAPSE_CONNECTION_STRING', 
                                               'DRIVER={ODBC Driver 17 for SQL Server};'
                                               'Server=tcp:seafar(confidential);'
                                               'Database=master;Uid=maria.chiara.bodda@seafar.eu;'
                                               'Encrypt=yes;Authentication=ActiveDirectoryInteractive;')
