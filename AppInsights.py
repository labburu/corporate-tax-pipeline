from databricks.sdk.runtime import *
import logging 
from applicationinsights import TelemetryClient

#initial setup for azure app insight connection
instrumentationKey = CLIENT_ID = dbutils.secrets.get(scope="SDE", key="appinsightsinstrumentationkey")
tc = TelemetryClient(instrumentation_key=instrumentationKey)

#setting up the logger
logging.basicConfig(level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())
tc.channel.logger = logging.getLogger(__name__)

#Send Message via sendLog function
def sendLog(logMsg):
  log_message = logMsg
  tc.track_trace(log_message)
  tc.flush()