using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;

using Inflectra.SpiraTest.PlugIns.MsTfsDataSync.SpiraImportExport;

using Microsoft.TeamFoundation;
using Microsoft.TeamFoundation.Framework.Client;
using Microsoft.TeamFoundation.Client;
using Microsoft.TeamFoundation.Common;
using Microsoft.TeamFoundation.Server;
using Microsoft.TeamFoundation.WorkItemTracking;
using Microsoft.TeamFoundation.WorkItemTracking.Client;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Globalization;
using System.ServiceModel;
using Microsoft.TeamFoundation.Framework.Common;
using Microsoft.TeamFoundation.WorkItemTracking.Proxy;
using System.IO;

namespace Inflectra.SpiraTest.PlugIns.MsTfsDataSync
{
    /// <summary>
    /// Contains all the logic necessary to sync SpiraTest with Microsoft Team Foundation Server 2012
    /// </summary>
    public class DataSync : IDataSyncPlugIn
    {
        //Constant containing data-sync name and internal API URL suffix to access
        private const string DATA_SYNC_NAME = "MsTfsDataSync";

        //Special TFS fields that we map to Spira custom properties
        private const string TFS_SPECIAL_FIELD_AREA = "Area";
        private const string TFS_SPECIAL_FIELD_WORK_ITEM_ID = "TfsWorkItemId";

        //TFS non-core fields
        private const string TFS_FIELD_STEPS_TO_REPRODUCE = "Microsoft.VSTS.TCM.ReproSteps";
        private const string TFS_FIELD_DESCRIPTION_RICH_TEXT = "Microsoft.VSTS.Common.DescriptionHtml";
        private const string TFS_FIELD_PRIORITY = "Priority";
        private const string TFS_FIELD_SEVERITY = "Severity";
        private const string TFS_FIELD_COMPLETED_WORK = "Completed Work";
        private const string TFS_FIELD_START_DATE = "Start Date";
        private const string TFS_FIELD_FINISH_DATE = "Finish Date";

        private const string TFS_SPECIAL_FIELD_INCIDENT_ID = "Incident.ID";

        // Track whether Dispose has been called.
        private bool disposed = false;

        //Configuration data passed through from calling service
        private EventLog eventLog;
        private bool traceLogging;
        private int dataSyncSystemId;
        private string webServiceBaseUrl;
        private string internalLogin;
        private string internalPassword;
        private string connectionString;
        private string externalLogin;
        private string externalPassword;
        private int timeOffsetHours;
        private bool autoMapUsers;
        private string windowsDomain;
        private string artifactIdTfsField;
        private string incidentDetectorTfsField;
        List<string> taskWorkItemTypes = new List<string>();
        List<string> requirementWorkItemTypes = new List<string>();
        TeamFoundationIdentity[] tfsUsers = null;
        WorkItemServer workItemServer = null;

        /// <summary>
        /// Constructor, does nothing - all setup in the Setup() method instead
        /// </summary>
        public DataSync()
        {
            //Does Nothing - all setup in the Setup() method instead
        }

        /// <summary>
        /// Loads in all the configuration information passed from the calling service
        /// </summary>
        /// <param name="eventLog">Handle to the event log to use</param>
        /// <param name="dataSyncSystemId">The id of the plug-in used when accessing the mapping repository</param>
        /// <param name="webServiceBaseUrl">The base URL of the Spira web service</param>
        /// <param name="internalLogin">The login to Spira</param>
        /// <param name="internalPassword">The password used for the Spira login</param>
        /// <param name="connectionString">The URL to access the TFS server</param>
        /// <param name="externalLogin">The login used for accessing TFS</param>
        /// <param name="externalPassword">The password for the TFS login</param>
        /// <param name="timeOffsetHours">Any time offset to apply between Spira and TFS</param>
        /// <param name="autoMapUsers">Should we auto-map users</param>
        /// <param name="custom01">The name of the Windows Domain that the login belongs to</param>
        /// <param name="custom02">Comma-separated list of work item types that map to tasks</param>
        /// <param name="custom03">The name of the TFS field used to store Spira Incident IDs</param>
        /// <param name="custom04">The name of the TFS field used to store Spira Detector's name</param>
        /// <param name="custom05">Comma-separated list of work item types that map to requirements</param>
        public void Setup(
            EventLog eventLog,
            bool traceLogging,
            int dataSyncSystemId,
            string webServiceBaseUrl,
            string internalLogin,
            string internalPassword,
            string connectionString,
            string externalLogin,
            string externalPassword,
            int timeOffsetHours,
            bool autoMapUsers,
            string custom01,
            string custom02,
            string custom03,
            string custom04,
            string custom05
            )
        {
            //Make sure the object has not been already disposed
            if (this.disposed)
            {
                throw new ObjectDisposedException(DATA_SYNC_NAME + " has been disposed already.");
            }

            try
            {
                //Set the member variables from the passed-in values
                this.eventLog = eventLog;
                this.traceLogging = traceLogging;
                this.dataSyncSystemId = dataSyncSystemId;
                this.webServiceBaseUrl = webServiceBaseUrl;
                this.internalLogin = internalLogin;
                this.internalPassword = internalPassword;
                this.connectionString = connectionString;
                this.externalLogin = externalLogin;
                this.externalPassword = externalPassword;
                this.timeOffsetHours = timeOffsetHours;
                this.autoMapUsers = autoMapUsers;
                this.windowsDomain = custom01;
                this.artifactIdTfsField = custom03;
                this.incidentDetectorTfsField = custom04;

                //See if we have any work item types specified for mapping to tasks
                if (!String.IsNullOrWhiteSpace(custom02))
                {
                    string[] workItemTypes = custom02.Split(',');
                    foreach (string workItemType in workItemTypes)
                    {
                        this.taskWorkItemTypes.Add(workItemType.Trim());
                    }
                }

                //See if we have any work item types specified for mapping to requirements
                if (!String.IsNullOrWhiteSpace(custom05))
                {
                    string[] workItemTypes = custom05.Split(',');
                    foreach (string workItemType in workItemTypes)
                    {
                        this.requirementWorkItemTypes.Add(workItemType.Trim());
                    }
                }
            }
            catch (Exception exception)
            {
                //Log and rethrow the exception
                eventLog.WriteEntry("Unable to setup the " + DATA_SYNC_NAME + " plug-in ('" + exception.Message + "')\n" + exception.StackTrace, EventLogEntryType.Error);
                throw exception;
            }
        }

        /// <summary>
        /// Executes the data-sync functionality between the two systems
        /// </summary>
        /// <param name="LastSyncDate">The last date/time the plug-in was successfully executed (in UTC)</param>
        /// <param name="serverDateTime">The current date/time on the server (in UTC)</param>
        /// <returns>Code denoting success, failure or warning</returns>
        public ServiceReturnType Execute(Nullable<DateTime> lastSyncDate, DateTime serverDateTime)
        {
            //Make sure the object has not been already disposed
            if (this.disposed)
            {
                throw new ObjectDisposedException(DATA_SYNC_NAME + " has been disposed already.");
            }

            try
            {
                LogTraceEvent(eventLog, "Starting " + DATA_SYNC_NAME + " data synchronization", EventLogEntryType.Information);

                //Instantiate the SpiraTest web-service proxy class
                Uri spiraUri = new Uri(this.webServiceBaseUrl + Constants.WEB_SERVICE_URL_SUFFIX);
                SpiraImportExport.ImportExportClient spiraImportExport = SpiraClientFactory.CreateClient(spiraUri);

                //First lets get the product name we should be referring to
                string productName = spiraImportExport.System_GetProductName();

                //**** Next lets load in the project and user mappings ****
                bool success = spiraImportExport.Connection_Authenticate2(internalLogin, internalPassword, DATA_SYNC_NAME);
                if (!success)
                {
                    //We can't authenticate so end
                    LogErrorEvent("Unable to authenticate with " + productName + " API, stopping data-synchronization", EventLogEntryType.Error);
                    return ServiceReturnType.Error;
                }
                SpiraImportExport.RemoteDataMapping[] projectMappings = spiraImportExport.DataMapping_RetrieveProjectMappings(dataSyncSystemId);
                SpiraImportExport.RemoteDataMapping[] userMappings = spiraImportExport.DataMapping_RetrieveUserMappings(dataSyncSystemId);

                //Configure the network credentials - used for accessing the MsTfs API
                //If we have a domain provided, use a NetworkCredential, otherwise use a TFS credential
                TfsClientCredentials tfsCredential = null;
                NetworkCredential networkCredential = null;
                if (String.IsNullOrEmpty(this.windowsDomain))
                {
                    //Windows Live credentials
                    networkCredential = new NetworkCredential(externalLogin, externalPassword);
                    BasicAuthCredential basicCredential = new BasicAuthCredential(networkCredential);
                    tfsCredential = new TfsClientCredentials(basicCredential);
                    tfsCredential.AllowInteractive = false;
                }
                else
                {
                    //Windows credentials
                    networkCredential = new NetworkCredential(this.externalLogin, this.externalPassword, this.windowsDomain);
                }

                //Create a new TFS 2012 project collection instance and WorkItemStore instance
                //This requires that the URI includes the collection name not just the server name
                WorkItemStore workItemStore = null;
                Uri tfsUri = new Uri(this.connectionString);
                TfsTeamProjectCollection tfsTeamProjectCollection;
                if (String.IsNullOrEmpty(windowsDomain))
                {
                    tfsTeamProjectCollection = new TfsTeamProjectCollection(tfsUri, tfsCredential);
                }
                else
                {
                    tfsTeamProjectCollection = new TfsTeamProjectCollection(tfsUri, networkCredential);
                }
                LogTraceEvent("Created new TFS Project Collection instance");

                //Get access to the work item server service
                this.workItemServer = tfsTeamProjectCollection.GetService<WorkItemServer>();
                LogTraceEvent("Got access to the WorkItemServer service");

                //Get the global security service to retrieve the TFS user list
                IIdentityManagementService ims = (IIdentityManagementService)tfsTeamProjectCollection.GetService(typeof(IIdentityManagementService));
                if (ims != null)
                {
                    TeamFoundationIdentity SIDS = ims.ReadIdentity(IdentitySearchFactor.AccountName, "Project Collection Valid Users", MembershipQuery.None, ReadIdentityOptions.IncludeReadFromSource);
                    if (SIDS != null)
                    {
                        this.tfsUsers = ims.ReadIdentities(SIDS.Members, MembershipQuery.Expanded, ReadIdentityOptions.ExtendedProperties);
                    }
                }
                LogTraceEvent("Got access to the IIdentityManagementService");

                //Get access to the work item store
                try
                {
                    tfsTeamProjectCollection.Authenticate();
                    workItemStore = new WorkItemStore(tfsTeamProjectCollection);
                }
                catch (Exception exception)
                {
                    //We can't authenticate so end
                    eventLog.WriteEntry("Unable to connect to Team Foundation Server, please check that the connection information is correct (" + exception.Message + ")", EventLogEntryType.Error);
                    eventLog.WriteEntry(exception.Message + ": " + exception.StackTrace);
                    if (exception.InnerException != null)
                    {
                        LogErrorEvent("Inner Exception=" + exception.InnerException.Message + ": " + exception.InnerException.StackTrace);
                    }
                    return ServiceReturnType.Error;
                }
                if (workItemStore == null)
                {
                    //We can't authenticate so end
                    LogErrorEvent("Unable to connect to Team Foundation Server, please check that the connection information is correct", EventLogEntryType.Error);
                    return ServiceReturnType.Error;
                }
                LogTraceEvent("Got access to the WorkItemStore");

                //Loop for each of the projects in the project mapping
                foreach (SpiraImportExport.RemoteDataMapping projectMapping in projectMappings)
                {
                    try
                    {
                        //Get the SpiraTest project id equivalent TFS project identifier
                        int projectId = projectMapping.InternalId;
                        string tfsProject = projectMapping.ExternalKey;

                        //Re-authenticate with Spira to avoid potential timeout issues
                        success = spiraImportExport.Connection_Authenticate2(internalLogin, internalPassword, DATA_SYNC_NAME);
                        if (!success)
                        {
                            //We can't authenticate so end
                            LogErrorEvent("Unable to authenticate with " + productName + " API, stopping data-synchronization", EventLogEntryType.Error);
                            return ServiceReturnType.Error;
                        }

                        //Connect to the SpiraTest project
                        success = spiraImportExport.Connection_ConnectToProject(projectId);
                        if (!success)
                        {
                            //We can't connect so go to next project
                            eventLog.WriteEntry(String.Format("Unable to connect to {0} project PR{1}, please check that the {0} login has the appropriate permissions", productName, projectId), EventLogEntryType.Error);
                            continue;
                        }

                        //Connect to the TFS project
                        Project project = workItemStore.Projects[tfsProject];

                        //Get the list of project-specific mappings from the data-mapping repository
                        //Incidents
                        SpiraImportExport.RemoteDataMapping[] incidentSeverityMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Incident_Severity);
                        SpiraImportExport.RemoteDataMapping[] incidentPriorityMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Incident_Priority);
                        SpiraImportExport.RemoteDataMapping[] incidentStatusMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Incident_Status);
                        SpiraImportExport.RemoteDataMapping[] incidentTypeMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Incident_Type);
                        //Tasks
                        SpiraImportExport.RemoteDataMapping[] taskPriorityMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Task_Priority);
                        SpiraImportExport.RemoteDataMapping[] taskStatusMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Task_Status);
                        //Requirements
                        SpiraImportExport.RemoteDataMapping[] requirementImportanceMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Requirement_Importance);
                        SpiraImportExport.RemoteDataMapping[] requirementStatusMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Requirement_Status);

                        //Get the list of custom properties configured for this project and the corresponding data mappings

                        //First for incidents
                        RemoteCustomProperty[] incidentCustomProperties = spiraImportExport.CustomProperty_RetrieveForArtifactType((int)Constants.ArtifactType.Incident, false);
                        Dictionary<int, RemoteDataMapping> incidentCustomPropertyMappingList = new Dictionary<int, SpiraImportExport.RemoteDataMapping>();
                        Dictionary<int, RemoteDataMapping[]> incidentCustomPropertyValueMappingList = new Dictionary<int, SpiraImportExport.RemoteDataMapping[]>();
                        foreach (SpiraImportExport.RemoteCustomProperty customProperty in incidentCustomProperties)
                        {
                            //Get the mapping for this custom property
                            if (customProperty.CustomPropertyId.HasValue)
                            {
                                SpiraImportExport.RemoteDataMapping customPropertyMapping = spiraImportExport.DataMapping_RetrieveCustomPropertyMapping(dataSyncSystemId, (int)Constants.ArtifactType.Incident, customProperty.CustomPropertyId.Value);
                                incidentCustomPropertyMappingList.Add(customProperty.CustomPropertyId.Value, customPropertyMapping);

                                //For list types need to also get the property value mappings
                                if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.List || customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.MultiList)
                                {
                                    SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = spiraImportExport.DataMapping_RetrieveCustomPropertyValueMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident, customProperty.CustomPropertyId.Value);
                                    incidentCustomPropertyValueMappingList.Add(customProperty.CustomPropertyId.Value, customPropertyValueMappings);
                                }
                            }
                        }

                        //Next for requirements
                        RemoteCustomProperty[] requirementCustomProperties = spiraImportExport.CustomProperty_RetrieveForArtifactType((int)Constants.ArtifactType.Requirement, false);
                        Dictionary<int, RemoteDataMapping> requirementCustomPropertyMappingList = new Dictionary<int, SpiraImportExport.RemoteDataMapping>();
                        Dictionary<int, RemoteDataMapping[]> requirementCustomPropertyValueMappingList = new Dictionary<int, SpiraImportExport.RemoteDataMapping[]>();
                        foreach (SpiraImportExport.RemoteCustomProperty customProperty in requirementCustomProperties)
                        {
                            //Get the mapping for this custom property
                            if (customProperty.CustomPropertyId.HasValue)
                            {
                                SpiraImportExport.RemoteDataMapping customPropertyMapping = spiraImportExport.DataMapping_RetrieveCustomPropertyMapping(dataSyncSystemId, (int)Constants.ArtifactType.Requirement, customProperty.CustomPropertyId.Value);
                                requirementCustomPropertyMappingList.Add(customProperty.CustomPropertyId.Value, customPropertyMapping);

                                //For list types need to also get the property value mappings
                                if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.List || customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.MultiList)
                                {
                                    SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = spiraImportExport.DataMapping_RetrieveCustomPropertyValueMappings(dataSyncSystemId, (int)Constants.ArtifactType.Requirement, customProperty.CustomPropertyId.Value);
                                    requirementCustomPropertyValueMappingList.Add(customProperty.CustomPropertyId.Value, customPropertyValueMappings);
                                }
                            }
                        }

                        //Next for tasks
                        RemoteCustomProperty[] taskCustomProperties = spiraImportExport.CustomProperty_RetrieveForArtifactType((int)Constants.ArtifactType.Task, false);
                        Dictionary<int, RemoteDataMapping> taskCustomPropertyMappingList = new Dictionary<int, SpiraImportExport.RemoteDataMapping>();
                        Dictionary<int, RemoteDataMapping[]> taskCustomPropertyValueMappingList = new Dictionary<int, SpiraImportExport.RemoteDataMapping[]>();
                        foreach (SpiraImportExport.RemoteCustomProperty customProperty in taskCustomProperties)
                        {
                            //Get the mapping for this custom property
                            if (customProperty.CustomPropertyId.HasValue)
                            {
                                SpiraImportExport.RemoteDataMapping customPropertyMapping = spiraImportExport.DataMapping_RetrieveCustomPropertyMapping(dataSyncSystemId, (int)Constants.ArtifactType.Requirement, customProperty.CustomPropertyId.Value);
                                taskCustomPropertyMappingList.Add(customProperty.CustomPropertyId.Value, customPropertyMapping);

                                //For list types need to also get the property value mappings
                                if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.List || customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.MultiList)
                                {
                                    SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = spiraImportExport.DataMapping_RetrieveCustomPropertyValueMappings(dataSyncSystemId, (int)Constants.ArtifactType.Requirement, customProperty.CustomPropertyId.Value);
                                    taskCustomPropertyValueMappingList.Add(customProperty.CustomPropertyId.Value, customPropertyValueMappings);
                                }
                            }
                        }

                        //Now get the list of releases and incidents that have already been mapped
                        RemoteDataMapping[] requirementMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Requirement);
                        RemoteDataMapping[] taskMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Task);
                        RemoteDataMapping[] incidentMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident);
                        RemoteDataMapping[] releaseMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release);

                        //If we don't have a last-sync data, default to 1/1/1950
                        if (!lastSyncDate.HasValue)
                        {
                            lastSyncDate = DateTime.ParseExact("1/1/1950", "M/d/yyyy", CultureInfo.InvariantCulture);
                        }

                        //Get the incidents from SpiraTest in batches of 100
                        List<RemoteIncident> incidentList = new List<RemoteIncident>();
                        long incidentCount = spiraImportExport.Incident_Count(null);
                        for (int startRow = 1; startRow <= incidentCount; startRow += Constants.INCIDENT_PAGE_SIZE)
                        {
                            RemoteIncident[] incidentBatch = spiraImportExport.Incident_RetrieveNew(lastSyncDate.Value, startRow, Constants.INCIDENT_PAGE_SIZE);
                            incidentList.AddRange(incidentBatch);
                        }
                        LogTraceEvent(eventLog, "Found " + incidentList.Count + " new incidents in " + productName, EventLogEntryType.Information);

                        //Create the mapping collections to hold any new items that need to get added to the mappings
                        //or any old items that need to get removed from the mappings
                        List<RemoteDataMapping> newIncidentMappings = new List<SpiraImportExport.RemoteDataMapping>();
                        List<RemoteDataMapping> newReleaseMappings = new List<SpiraImportExport.RemoteDataMapping>();
                        List<RemoteDataMapping> oldReleaseMappings = new List<SpiraImportExport.RemoteDataMapping>();

                        //Iterate through each record
                        foreach (SpiraImportExport.RemoteIncident remoteIncident in incidentList)
                        {
                            try
                            {
                                ProcessNewIncident(projectId, spiraImportExport, remoteIncident, newIncidentMappings, newReleaseMappings, oldReleaseMappings, incidentCustomPropertyMappingList, incidentCustomPropertyValueMappingList, incidentCustomProperties, requirementMappings, incidentMappings, taskMappings, project, workItemStore, productName, incidentSeverityMappings, incidentPriorityMappings, incidentStatusMappings, incidentTypeMappings, userMappings, releaseMappings, tfsTeamProjectCollection);
                            }
                            catch (FaultException<ValidationFaultMessage> validationException)
                            {
                                string message = "";
                                ValidationFaultMessage validationFaultMessage = validationException.Detail;
                                message = validationFaultMessage.Summary + ": \n";
                                {
                                    foreach (ValidationFaultMessageItem messageItem in validationFaultMessage.Messages)
                                    {
                                        message += messageItem.FieldName + "=" + messageItem.Message + " \n";
                                    }
                                }
                                LogErrorEvent("Error Adding " + productName + " Incident to TFS. Validation messages = (" + message + ")\n" + validationException.StackTrace, EventLogEntryType.Error);
                            }
                            catch (Exception exception)
                            {
                                //Log and continue execution
                                LogErrorEvent("Error Adding " + productName + " Incident to TFS: " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                            }
                        }

                        //Re-authenticate with Spira and reconnect to the project to avoid potential timeout issues
                        success = spiraImportExport.Connection_Authenticate2(internalLogin, internalPassword, DATA_SYNC_NAME);
                        if (!success)
                        {
                            //We can't authenticate so end
                            LogErrorEvent("Unable to authenticate with " + productName + " API, stopping data-synchronization", EventLogEntryType.Error);
                            return ServiceReturnType.Error;
                        }
                        success = spiraImportExport.Connection_ConnectToProject(projectId);
                        if (!success)
                        {
                            //We can't connect so go to next project
                            LogErrorEvent("Unable to connect to " + productName + " project PR" + projectId + ", please check that the " + productName + " login has the appropriate permissions", EventLogEntryType.Error);
                            return ServiceReturnType.Error;
                        }

                        //Finally we need to update the mapping data on the server before starting the second phase
                        //of the data-synchronization
                        //At this point we have potentially added incidents, added releases and removed releases
                        spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident, newIncidentMappings.ToArray());
                        spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release, newReleaseMappings.ToArray());
                        spiraImportExport.DataMapping_RemoveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release, oldReleaseMappings.ToArray());

                        //**** Next we need to get a list of any new/updated work items from TFS ****
                        incidentMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident);
                        requirementMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Requirement);
                        taskMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Task);

                        //Need to create a list to hold any new releases, requirements, tasks and incidents
                        newIncidentMappings = new List<SpiraImportExport.RemoteDataMapping>();
                        newReleaseMappings = new List<SpiraImportExport.RemoteDataMapping>();
                        List<RemoteDataMapping> newRequirementMappings = new List<RemoteDataMapping>();
                        List<RemoteDataMapping> newTaskMappings = new List<RemoteDataMapping>();

                        //**** Next we need to see if any new work items were logged in TFS ****
                        //We need to convert from UTC to local time since the TFS API expects the date-time in local time
                        DateTime startingDate = lastSyncDate.Value.AddHours(-timeOffsetHours).ToLocalTime();
                        string fieldList = "";
                        foreach (FieldDefinition field in workItemStore.FieldDefinitions)
                        {
                            if (fieldList != "")
                            {
                                fieldList += ",";
                            }
                            fieldList += "[" + field.Name + "]";
                        }

                        WorkItemCollection workItemCollection = null;
                        string wiqlQuery = "SELECT " + fieldList + " FROM WorkItems WHERE [System.CreatedDate] >= '" + startingDate.ToShortDateString() + "' AND [System.TeamProject] = '" + tfsProject + "' ORDER BY [System.CreatedDate]";
                        try
                        {
                            workItemCollection = workItemStore.Query(wiqlQuery);
                        }
                        catch (Exception exception)
                        {
                            //See if we have the exception about exceeding the number of items (error VS402337)
                            if (exception.Message.Contains("VS402337"))
                            {
                                //Just look back in time 2-days instead of the specified date
                                startingDate = DateTime.Now.AddDays(-2);
                                wiqlQuery = "SELECT " + fieldList + " FROM WorkItems WHERE [System.CreatedDate] >= '" + startingDate.ToShortDateString() + "' AND [System.TeamProject] = '" + tfsProject + "' ORDER BY [System.CreatedDate]";
                                workItemCollection = workItemStore.Query(wiqlQuery);
                                LogErrorEvent("Exceeded TFS work item query limit so only looking back 2 days (" + exception.Message + ")", EventLogEntryType.Warning);
                            }
                            else
                            {
                                LogErrorEvent("Error querying for TFS work items, error: " + exception.Message, EventLogEntryType.Error);
                            }
                        }

                        if (workItemCollection != null)
                        {
                            LogTraceEvent(eventLog, "Found " + workItemCollection.Count + " new work items in TFS", EventLogEntryType.Information);
                            foreach (WorkItem workItem in workItemCollection)
                            {
                                try
                                {
                                    //Open the work item
                                    workItem.Open();

                                    //See if this work item should be treated as an incident, task or requirement
                                    if (workItem.Type != null && this.requirementWorkItemTypes.Contains(workItem.Type.Name))
                                    {
                                        ProcessNewWorkItemAsRequirement(projectId, spiraImportExport, workItem, newRequirementMappings, newReleaseMappings, oldReleaseMappings, requirementCustomPropertyMappingList, requirementCustomPropertyValueMappingList, requirementCustomProperties, requirementMappings, incidentMappings, taskMappings, project, workItemStore, productName, requirementImportanceMappings, requirementStatusMappings, userMappings, releaseMappings);
                                    }
                                    else if (workItem.Type != null && this.taskWorkItemTypes.Contains(workItem.Type.Name))
                                    {
                                        ProcessNewWorkItemAsTask(projectId, spiraImportExport, workItem, newTaskMappings, newReleaseMappings, oldReleaseMappings, taskCustomPropertyMappingList, taskCustomPropertyValueMappingList, taskCustomProperties, requirementMappings, incidentMappings, taskMappings, project, workItemStore, productName, taskPriorityMappings, taskStatusMappings, userMappings, releaseMappings);
                                    }
                                    else
                                    {
                                        ProcessNewWorkItemAsIncident(projectId, spiraImportExport, workItem, newIncidentMappings, newReleaseMappings, oldReleaseMappings, incidentCustomPropertyMappingList, incidentCustomPropertyValueMappingList, incidentCustomProperties, requirementMappings, incidentMappings, taskMappings, project, workItemStore, productName, incidentSeverityMappings, incidentPriorityMappings, incidentStatusMappings, incidentTypeMappings, userMappings, releaseMappings);
                                    }
                                }
                                catch (FaultException<ValidationFaultMessage> validationException)
                                {
                                    string message = "";
                                    ValidationFaultMessage validationFaultMessage = validationException.Detail;
                                    message = validationFaultMessage.Summary + ": \n";
                                    {
                                        foreach (ValidationFaultMessageItem messageItem in validationFaultMessage.Messages)
                                        {
                                            message += messageItem.FieldName + "=" + messageItem.Message + " \n";
                                        }
                                    }
                                    LogErrorEvent("Error adding TFS work item " + workItem.Id + " to " + productName + " - validation messages = (" + message + ")\n" + validationException.StackTrace, EventLogEntryType.Error);
                                }
                                catch (Exception exception)
                                {
                                    //Log the error and move on to the next item
                                    LogErrorEvent("Error adding TFS work item " + workItem.Id + " to " + productName + " - error message = " + exception.Message, EventLogEntryType.Error);
                                }
                            }
                        }

                        //Re-authenticate with Spira and reconnect to the project to avoid potential timeout issues
                        success = spiraImportExport.Connection_Authenticate2(internalLogin, internalPassword, DATA_SYNC_NAME);
                        if (!success)
                        {
                            //We can't authenticate so end
                            LogErrorEvent("Unable to authenticate with " + productName + " API, stopping data-synchronization", EventLogEntryType.Error);
                            return ServiceReturnType.Error;
                        }
                        success = spiraImportExport.Connection_ConnectToProject(projectId);
                        if (!success)
                        {
                            //We can't connect so go to next project
                            LogErrorEvent("Unable to connect to " + productName + " project PR" + projectId + ", please check that the " + productName + " login has the appropriate permissions", EventLogEntryType.Error);
                            return ServiceReturnType.Error;
                        }

                        //Next we need to update the mapping data on the server
                        //At this point we have potentially added releases, requirements, tasks and incidents
                        spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release, newReleaseMappings.ToArray());
                        spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident, newIncidentMappings.ToArray());
                        spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Requirement, newRequirementMappings.ToArray());
                        spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Task, newTaskMappings.ToArray());

                        //Now we need to update any existing mapped items

                        //Re-authenticate with Spira and reconnect to the project to avoid potential timeout issues
                        success = spiraImportExport.Connection_Authenticate2(internalLogin, internalPassword, DATA_SYNC_NAME);
                        if (!success)
                        {
                            //We can't authenticate so end
                            LogErrorEvent("Unable to authenticate with " + productName + " API, stopping data-synchronization", EventLogEntryType.Error);
                            return ServiceReturnType.Error;
                        }
                        success = spiraImportExport.Connection_ConnectToProject(projectId);
                        if (!success)
                        {
                            //We can't connect so go to next project
                            LogErrorEvent("Unable to connect to " + productName + " project PR" + projectId + ", please check that the " + productName + " login has the appropriate permissions", EventLogEntryType.Error);
                            return ServiceReturnType.Error;
                        }

                        //Need to create a list to hold any new releases or obsolete releases
                        newReleaseMappings = new List<SpiraImportExport.RemoteDataMapping>();
                        oldReleaseMappings = new List<SpiraImportExport.RemoteDataMapping>();

                        //Get the updated set of artifact data mappings
                        incidentMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident);
                        requirementMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Requirement);
                        taskMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Task);
                        releaseMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release);

                        //See if any incidents have changed in Spira since the last sync, we don't update requirements and tasks in the other direction,
                        //Also exclude those incidents that were added since the last sync
                        //so we only need to do this for incidents
                        List<int> internalIds = new List<int>();
                        RemoteSort remoteSort = new RemoteSort();
                        remoteSort.PropertyName = "LastUpdateDate";
                        remoteSort.SortAscending = true;
                        RemoteFilter lastUpdateFilter = new RemoteFilter();
                        lastUpdateFilter.PropertyName = "LastUpdateDate";
                        lastUpdateFilter.DateRangeValue = new SpiraImportExport.DateRange();
                        lastUpdateFilter.DateRangeValue.ConsiderTimes = true;
                        lastUpdateFilter.DateRangeValue.StartDate = lastSyncDate.Value;
                        lastUpdateFilter.DateRangeValue.EndDate = null;
                        RemoteFilter creationFilter = new RemoteFilter();
                        creationFilter.PropertyName = "CreationDate";
                        creationFilter.DateRangeValue = new SpiraImportExport.DateRange();
                        creationFilter.DateRangeValue.ConsiderTimes = true;
                        creationFilter.DateRangeValue.StartDate = null;
                        creationFilter.DateRangeValue.EndDate = lastSyncDate.Value;
                        List<RemoteFilter> remoteFilters = new List<RemoteFilter>();
                        remoteFilters.Add(lastUpdateFilter);
                        remoteFilters.Add(creationFilter);
                        for (int startRow = 1; startRow <= incidentCount; startRow += Constants.INCIDENT_PAGE_SIZE)
                        {
                            RemoteIncident[] incidentBatch = spiraImportExport.Incident_Retrieve(remoteFilters.ToArray(), remoteSort, startRow, Constants.INCIDENT_PAGE_SIZE);
                            foreach (RemoteIncident remoteIncident in incidentBatch)
                            {
                                internalIds.Add(remoteIncident.IncidentId.Value);
                            }
                        }
                        LogTraceEvent(eventLog, "Found " + internalIds.Count + " updated incidents in " + productName, EventLogEntryType.Information);

                        //See if any work items have changed in TFS since the last sync that were not created recently
                        //We only want the IDs at this point
                        wiqlQuery = "SELECT [System.ID] FROM WorkItems WHERE [System.ChangedDate] >= '" + startingDate.ToShortDateString() + "' AND [System.TeamProject] = '" + tfsProject + "' ORDER BY [System.ChangedDate]";
                        workItemCollection = null;
                        try
                        {
                            workItemCollection = workItemStore.Query(wiqlQuery);
                        }
                        catch (Exception exception)
                        {
                            //See if we have the exception about exceeding the number of items (error VS402337)
                            if (exception.Message.Contains("VS402337"))
                            {
                                //Just look back in time 2-days instead of the specified date
                                startingDate = DateTime.Now.AddDays(-2);
                                wiqlQuery = "SELECT [System.ID] FROM WorkItems WHERE [System.ChangedDate] >= '" + startingDate.ToShortDateString() + "' AND [System.TeamProject] = '" + tfsProject + "' ORDER BY [System.ChangedDate]";
                                workItemCollection = workItemStore.Query(wiqlQuery);
                                LogErrorEvent("Exceeded TFS work item query limit so only looking back 2 days (" + exception.Message + ")", EventLogEntryType.Warning);
                            }
                            else
                            {
                                LogErrorEvent("Error querying for TFS work items, error: " + exception.Message, EventLogEntryType.Error);
                            }
                        }

                        List<string> externalIds = new List<string>();
                        if (workItemCollection != null)
                        {
                            foreach (WorkItem workItem in workItemCollection)
                            {
                                externalIds.Add(workItem.Id.ToString());
                            }
                            LogTraceEvent(eventLog, "Found " + externalIds.Count + " updated work items in TFS", EventLogEntryType.Information);
                        }

                        //Now we need to consolidate these two links into a single one that has both IDs
                        List<RemoteDataMapping> incidentsThatHaveUpdated = new List<SpiraImportExport.RemoteDataMapping>();
                        List<RemoteDataMapping> requirementsThatHaveUpdated = new List<SpiraImportExport.RemoteDataMapping>();
                        List<RemoteDataMapping> tasksThatHaveUpdated = new List<SpiraImportExport.RemoteDataMapping>();

                        //First Spira items
                        foreach (int internalId in internalIds)
                        {
                            RemoteDataMapping artifactMapping = InternalFunctions.FindMappingByInternalId(internalId, incidentMappings);
                            if (artifactMapping != null && InternalFunctions.FindMappingByInternalId(internalId, incidentsThatHaveUpdated.ToArray()) == null)
                            {
                                incidentsThatHaveUpdated.Add(artifactMapping);
                            }
                        }

                        //Next TFS items
                        foreach (string externalId in externalIds)
                        {
                            RemoteDataMapping artifactMapping = InternalFunctions.FindMappingByExternalKey(externalId, incidentMappings);
                            if (artifactMapping != null && InternalFunctions.FindMappingByExternalKey(externalId, incidentsThatHaveUpdated.ToArray()) == null)
                            {
                                incidentsThatHaveUpdated.Add(artifactMapping);
                            }
                            else
                            {
                                artifactMapping = InternalFunctions.FindMappingByExternalKey(externalId, taskMappings);
                                if (artifactMapping != null && taskWorkItemTypes.Count > 0 && InternalFunctions.FindMappingByExternalKey(externalId, tasksThatHaveUpdated.ToArray()) == null)
                                {
                                    tasksThatHaveUpdated.Add(artifactMapping);
                                }
                                else
                                {
                                    artifactMapping = InternalFunctions.FindMappingByExternalKey(externalId, requirementMappings);
                                    if (artifactMapping != null && requirementWorkItemTypes.Count > 0 && InternalFunctions.FindMappingByExternalKey(externalId, requirementsThatHaveUpdated.ToArray()) == null)
                                    {
                                        requirementsThatHaveUpdated.Add(artifactMapping);
                                    }
                                }
                            }
                        }

                        //Log the list of updates to be made
                        LogTraceEvent(eventLog, "Found " + incidentsThatHaveUpdated.Count + " incidents that have been updated", EventLogEntryType.Information);
                        LogTraceEvent(eventLog, "Found " + requirementsThatHaveUpdated.Count + " requirements that have been updated", EventLogEntryType.Information);
                        LogTraceEvent(eventLog, "Found " + tasksThatHaveUpdated.Count + " tasks that have been updated", EventLogEntryType.Information);

                        //Now that we have the deconflicted list of items that have changed in either system, iterate through and make the necessary updates
                        //Incidents
                        foreach (RemoteDataMapping artifactMapping in incidentsThatHaveUpdated)
                        {
                            try
                            {
                                ProcessUpdatedIncident(projectId, spiraImportExport, artifactMapping, newReleaseMappings, oldReleaseMappings, incidentCustomPropertyMappingList, incidentCustomPropertyValueMappingList, incidentCustomProperties, tfsTeamProjectCollection, project, workItemStore, productName, incidentSeverityMappings, incidentPriorityMappings, incidentStatusMappings, incidentTypeMappings, userMappings, releaseMappings);
                            }
                            catch (FaultException<ValidationFaultMessage> validationException)
                            {
                                string message = "";
                                ValidationFaultMessage validationFaultMessage = validationException.Detail;
                                message = validationFaultMessage.Summary + ": \n";
                                {
                                    foreach (ValidationFaultMessageItem messageItem in validationFaultMessage.Messages)
                                    {
                                        message += messageItem.FieldName + "=" + messageItem.Message + " \n";
                                    }
                                }
                                LogErrorEvent("Error updating TFS work item in " + productName + " - validation messages = " + productName + " (" + message + ")\n" + validationException.StackTrace, EventLogEntryType.Error);
                            }
                            catch (Exception exception)
                            {
                                //Log the error and move on to the next item
                                LogErrorEvent("Error updating TFS work item in " + productName + " - error message = " + exception.Message, EventLogEntryType.Error);
                            }
                        }

                        //Tasks
                        foreach (RemoteDataMapping artifactMapping in tasksThatHaveUpdated)
                        {
                            try
                            {
                                ProcessUpdatedTask(projectId, spiraImportExport, artifactMapping, newReleaseMappings, oldReleaseMappings, taskCustomPropertyMappingList, taskCustomPropertyValueMappingList, taskCustomProperties, project, workItemStore, productName, taskPriorityMappings, taskStatusMappings, userMappings, releaseMappings);
                            }
                            catch (FaultException<ValidationFaultMessage> validationException)
                            {
                                string message = "";
                                ValidationFaultMessage validationFaultMessage = validationException.Detail;
                                message = validationFaultMessage.Summary + ": \n";
                                {
                                    foreach (ValidationFaultMessageItem messageItem in validationFaultMessage.Messages)
                                    {
                                        message += messageItem.FieldName + "=" + messageItem.Message + " \n";
                                    }
                                }
                                LogErrorEvent("Error updating TFS work item in " + productName + " - validation messages = " + productName + " (" + message + ")\n" + validationException.StackTrace, EventLogEntryType.Error);
                            }
                            catch (Exception exception)
                            {
                                //Log the error and move on to the next item
                                LogErrorEvent("Error updating TFS work item in " + productName + " - error message = " + exception.Message, EventLogEntryType.Error);
                            }
                        }

                        //Requirements
                        foreach (RemoteDataMapping artifactMapping in requirementsThatHaveUpdated)
                        {
                            try
                            {
                                ProcessUpdatedRequirement(projectId, spiraImportExport, artifactMapping, newReleaseMappings, oldReleaseMappings, requirementCustomPropertyMappingList, requirementCustomPropertyValueMappingList, requirementCustomProperties, project, workItemStore, productName, requirementImportanceMappings, requirementStatusMappings, userMappings, releaseMappings);
                            }
                            catch (FaultException<ValidationFaultMessage> validationException)
                            {
                                string message = "";
                                ValidationFaultMessage validationFaultMessage = validationException.Detail;
                                message = validationFaultMessage.Summary + ": \n";
                                {
                                    foreach (ValidationFaultMessageItem messageItem in validationFaultMessage.Messages)
                                    {
                                        message += messageItem.FieldName + "=" + messageItem.Message + " \n";
                                    }
                                }
                                LogErrorEvent("Error updating TFS work item in " + productName + " - validation messages = " + productName + " (" + message + ")\n" + validationException.StackTrace, EventLogEntryType.Error);
                            }
                            catch (Exception exception)
                            {
                                //Log the error and move on to the next item
                                LogErrorEvent("Error updating TFS work item in " + productName + " - error message = " + exception.Message, EventLogEntryType.Error);
                            }
                        }

                        //Re-authenticate with Spira and reconnect to the project to avoid potential timeout issues
                        success = spiraImportExport.Connection_Authenticate2(internalLogin, internalPassword, DATA_SYNC_NAME);
                        if (!success)
                        {
                            //We can't authenticate so end
                            LogErrorEvent("Unable to authenticate with " + productName + " API, stopping data-synchronization", EventLogEntryType.Error);
                            return ServiceReturnType.Error;
                        }
                        success = spiraImportExport.Connection_ConnectToProject(projectId);
                        if (!success)
                        {
                            //We can't connect so go to next project
                            LogErrorEvent("Unable to connect to " + productName + " project PR" + projectId + ", please check that the " + productName + " login has the appropriate permissions", EventLogEntryType.Error);
                            return ServiceReturnType.Error;
                        }

                        //Next we need to update the mapping data on the server
                        //At this point we have potentially added and removed releases
                        spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release, newReleaseMappings.ToArray());
                        spiraImportExport.DataMapping_RemoveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release, oldReleaseMappings.ToArray());

                        //Clean up
                        project = null;

                        //Incidents
                        incidentSeverityMappings = null;
                        incidentPriorityMappings = null;
                        incidentStatusMappings = null;
                        incidentTypeMappings = null;
                        incidentCustomPropertyMappingList = null;
                        incidentCustomPropertyValueMappingList = null;
                        incidentMappings = null;
                        newIncidentMappings = null;

                        //Tasks
                        taskPriorityMappings = null;
                        taskStatusMappings = null;
                        taskCustomPropertyMappingList = null;
                        taskCustomPropertyValueMappingList = null;
                        taskMappings = null;
                        newTaskMappings = null;

                        //Requirements
                        requirementImportanceMappings = null;
                        requirementStatusMappings = null;
                        requirementCustomPropertyMappingList = null;
                        requirementCustomPropertyValueMappingList = null;
                        requirementMappings = null;
                        newRequirementMappings = null;

                        //Releases
                        releaseMappings = null;
                        newReleaseMappings = null;
                        oldReleaseMappings = null;
                    }
                    catch (Exception exception)
                    {
                        //Log the exception, but continue the sync
                        eventLog.WriteEntry("Error Synching project PR" + projectMapping.InternalId + ": " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                    }
                }

                //The following code is only needed during debugging
                LogTraceEvent(eventLog, "Import Completed", EventLogEntryType.Warning);

                //Mark objects ready for garbage collection
                tfsTeamProjectCollection.Dispose();
                spiraImportExport = null;
                tfsTeamProjectCollection = null;
                workItemStore = null;
                projectMappings = null;
                userMappings = null;
                tfsCredential = null;
                networkCredential = null;
                this.tfsUsers = null;
                this.workItemServer = null;

                //Let the service know that we ran correctly
                return ServiceReturnType.Success;
            }
            catch (Exception exception)
            {
                //Log the exception and return as a failure
                eventLog.WriteEntry("General Error: " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                return ServiceReturnType.Error;
            }
        }

        /// <summary>
        /// Validates a work item before we try and save it...
        /// </summary>
        /// <param name="item"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
        private bool ValidateItem(WorkItem item, StringBuilder messages)
        {

            bool isValid = true;

            foreach (Field field in item.Fields)
            {

                if (field.Status != FieldStatus.Valid)
                {

                    messages.Append(" \nField " + field.Name + " is invalid");

                    messages.Append(" \nStatus " + field.Status.ToString());

                    messages.Append(" \nValue " + field.Value.ToString());

                    if (field.Status == FieldStatus.InvalidListValue)
                    {

                        messages.Append(" \nAllowed Values are: \n");

                        foreach (object allowedValue in field.AllowedValues)
                        {

                            messages.AppendFormat("{0}\n", allowedValue.ToString());
                        }

                    }

                    isValid = false;

                }

            }

            return isValid;
        }

        /// <summary>
        /// Adds a new iteration to TFS
        /// </summary>
        /// <param name="project">The project object</param>
        /// <param name="iterationName">The name of the new iteration</param>
        /// <returns>The new iteration node</returns>
        /// <remarks>The new iteration is added under the root node</remarks>
        private Node AddNewTfsIteration(TfsTeamProjectCollection tfs, ref WorkItemStore workItemStore, ref Project project, string iterationName)
        {
            //Remove any invalid characters from the iteration name
            iterationName = System.Text.RegularExpressions.Regex.Replace(iterationName, @"[\\/$\?\*:""&><#%\|]", "");

            //Get the root node URI
            if (project.IterationRootNodes.Count == 0)
            {
                throw new Exception("You need to create at least one Iteration in TFS for using the Data-Sync");
            }
            string parentNodeUri = project.IterationRootNodes[0].ParentNode.Uri.AbsoluteUri;

            //Get the common service handle
            string nodeUri;
            try
            {
                ICommonStructureService commonStructureService = (ICommonStructureService)tfs.GetService(typeof(ICommonStructureService));
                nodeUri = commonStructureService.CreateNode(iterationName, parentNodeUri);
            }
            catch (Exception exception)
            {
                throw new Exception(String.Format("Unable to create a new TFS iteration with name '{0}' - error was: {1}", iterationName, exception.Message));
            }

            //Now get the node
            LogTraceEvent(eventLog, "Looking for matching node '" + nodeUri + "'", EventLogEntryType.Information);

            //It takes a while for the new node to become available, so need to wait for it to become so
            Node newIterationNode = null;
            while (newIterationNode == null)
            {
                workItemStore = new WorkItemStore(tfs);
                project = workItemStore.Projects[project.Name];
                newIterationNode = GetMatchingNode(project.IterationRootNodes, nodeUri);
                Thread.Sleep(1000);
            }
            return newIterationNode;
        }

        /// <summary>
        /// Gets the node with the specified ID
        /// </summary>
        /// <param name="nodeId">The ID of the node we want to find</param>
        /// <returns>The matching node</returns>
        private Node GetMatchingNode(NodeCollection nodeCollection, int nodeId)
        {
            foreach (Node currentNode in nodeCollection)
            {
                if (currentNode.Id == nodeId)
                {
                    return currentNode;
                }
                if (currentNode.ChildNodes.Count > 0)
                {
                    Node matchingNode = GetMatchingNode(currentNode.ChildNodes, nodeId);
                    if (matchingNode != null)
                    {
                        return matchingNode;
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Gets the node with the specified ID
        /// </summary>
        /// <param name="nodeUri">The URI of the node we want to find</param>
        /// <returns>The matching node</returns>
        private Node GetMatchingNode(NodeCollection nodeCollection, string nodeUri)
        {
            foreach (Node currentNode in nodeCollection)
            {
                LogTraceEvent(eventLog, "Current node '" + currentNode.Uri.AbsoluteUri + "'", EventLogEntryType.FailureAudit);
                if (currentNode.Uri.AbsoluteUri == nodeUri)
                {
                    return currentNode;
                }
                if (currentNode.ChildNodes.Count > 0)
                {
                    Node matchingNode = GetMatchingNode(currentNode.ChildNodes, nodeUri);
                    if (matchingNode != null)
                    {
                        return matchingNode;
                    }
                }
            }
            return null;
        }

        // Implement IDisposable.
        // Do not make this method virtual.
        // A derived class should not be able to override this method.
        public void Dispose()
        {
            Dispose(true);
            // Take yourself off the Finalization queue 
            // to prevent finalization code for this object
            // from executing a second time.
            GC.SuppressFinalize(this);
        }

        // Use C# destructor syntax for finalization code.
        // This destructor will run only if the Dispose method 
        // does not get called.
        // It gives your base class the opportunity to finalize.
        // Do not provide destructors in types derived from this class.
        ~DataSync()
        {
            // Do not re-create Dispose clean-up code here.
            // Calling Dispose(false) is optimal in terms of
            // readability and maintainability.
            Dispose(false);
        }

        // Dispose(bool disposing) executes in two distinct scenarios.
        // If disposing equals true, the method has been called directly
        // or indirectly by a user's code. Managed and unmanaged resources
        // can be disposed.
        // If disposing equals false, the method has been called by the 
        // runtime from inside the finalizer and you should not reference 
        // other objects. Only unmanaged resources can be disposed.
        protected virtual void Dispose(bool disposing)
        {
            // Check to see if Dispose has already been called.
            if (!this.disposed)
            {
                // If disposing equals true, dispose all managed 
                // and unmanaged resources.
                if (disposing)
                {
                    //Remove all references to member objects
                    this.eventLog = null;
                }
                // Release unmanaged resources. If disposing is false, 
                // only the following code is executed.

                //This class doesn't have any unmanaged resources to worry about
            }
            disposed = true;
        }

        /// <summary>
        /// Logs a trace event message if the configuration option is set
        /// </summary>
        /// <param name="eventLog">The event log handle</param>
        /// <param name="message">The message to log</param>
        /// <param name="type">The type of event</param>
        public void LogTraceEvent(EventLog eventLog, string message, EventLogEntryType type)
        {
            if (traceLogging && this.eventLog != null)
            {
                if (message.Length > 31000)
                {
                    //Split into smaller lengths
                    int index = 0;
                    while (index < message.Length)
                    {
                        try
                        {
                            string messageElement = message.Substring(index, 31000);
                            this.eventLog.WriteEntry(messageElement, type);
                        }
                        catch (ArgumentOutOfRangeException)
                        {
                            string messageElement = message.Substring(index);
                            this.eventLog.WriteEntry(messageElement, type);
                        }
                        index += 31000;
                    }
                }
                else
                {
                    this.eventLog.WriteEntry(message, type);
                }
            }
        }

        /// <summary>
        /// Logs a trace event message if the configuration option is set
        /// </summary>
        /// <param name="eventLog">The event log handle</param>
        /// <param name="message">The message to log</param>
        /// <param name="type">The type of event</param>
        public void LogTraceEvent(string message, EventLogEntryType type = EventLogEntryType.Information)
        {
            if (this.eventLog != null)
            {
                LogTraceEvent(this.eventLog, message, type);
            }
        }

        /// <summary>
        /// Logs an error event message
        /// </summary>
        /// <param name="message">The message to log</param>
        /// <param name="type">The type of event</param>
        public void LogErrorEvent(string message, EventLogEntryType type = EventLogEntryType.Error)
        {
            if (this.eventLog != null)
            {
                if (message.Length > 31000)
                {
                    //Split into smaller lengths
                    int index = 0;
                    while (index < message.Length)
                    {
                        try
                        {
                            string messageElement = message.Substring(index, 31000);
                            this.eventLog.WriteEntry(messageElement, type);
                        }
                        catch (ArgumentOutOfRangeException)
                        {
                            string messageElement = message.Substring(index);
                            this.eventLog.WriteEntry(messageElement, type);
                        }
                        index += 31000;
                    }
                }
                else
                {
                    this.eventLog.WriteEntry(message, type);
                }
            }
        }

        /// <summary>
        /// Finds a user mapping entry from the internal id
        /// </summary>
        /// <param name="internalId">The internal id</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <returns>The matching entry or Null if none found</returns>
        /// <remarks>If we are auto-mapping users, it will lookup the user-id instead</remarks>
        protected RemoteDataMapping FindUserMappingByInternalId(int internalId, RemoteDataMapping[] dataMappings, ImportExportClient client)
        {
            if (this.autoMapUsers)
            {
                RemoteUser remoteUser = client.User_RetrieveById(internalId);
                if (remoteUser == null)
                {
                    return null;
                }

                //The TFS API expects the display name not the login, so need to convert
                string tfsDisplayName = remoteUser.FullName;    //Use the full name from Spira as a backup
                if (this.tfsUsers != null)
                {
                    TeamFoundationIdentity matchedIdentity = this.tfsUsers.FirstOrDefault(i => i.UniqueName.ToLowerInvariant() == remoteUser.UserName.ToLowerInvariant());
                    if (matchedIdentity != null)
                    {
                        tfsDisplayName = matchedIdentity.DisplayName;
                    }
                }

                RemoteDataMapping userMapping = new RemoteDataMapping();
                userMapping.InternalId = remoteUser.UserId.Value;
                userMapping.ExternalKey = tfsDisplayName;
                return userMapping;
            }
            else
            {
                return InternalFunctions.FindMappingByInternalId(internalId, dataMappings);
            }
        }

        /// <summary>
        /// Finds a user mapping entry from the external key
        /// </summary>
        /// <param name="externalKey">The external key</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <returns>The matching entry or Null if none found</returns>
        /// <remarks>If we are auto-mapping users, it will lookup the username instead</remarks>
        protected RemoteDataMapping FindUserMappingByExternalKey(string externalKey, RemoteDataMapping[] dataMappings, ImportExportClient client)
        {
            if (this.autoMapUsers)
            {
                try
                {
                    //The TFS external key is the full name of the user, not their login, so we need to convert
                    string tfsLogin = externalKey;
                    if (this.tfsUsers != null)
                    {
                        TeamFoundationIdentity matchedIdentity = this.tfsUsers.FirstOrDefault(i => i.DisplayName.ToLowerInvariant() == externalKey.ToLowerInvariant());
                        if (matchedIdentity != null)
                        {
                            tfsLogin = matchedIdentity.UniqueName;
                        }
                    }

                    RemoteUser remoteUser = client.User_RetrieveByUserName(tfsLogin);
                    if (remoteUser == null)
                    {
                        return null;
                    }
                    RemoteDataMapping userMapping = new RemoteDataMapping();
                    userMapping.InternalId = remoteUser.UserId.Value;
                    userMapping.ExternalKey = remoteUser.UserName;
                    return userMapping;
                }
                catch (Exception)
                {
                    //User could not be found so return null
                    return null;
                }
            }
            else
            {
                return InternalFunctions.FindMappingByExternalKey(externalKey, dataMappings);
            }
        }

        /// <summary>
        /// Processes a new SpiraTest incident record
        /// </summary>
        /// <param name="remoteIncident">The Spira incident</param>
        private void ProcessNewIncident(int projectId, ImportExportClient spiraImportExport, RemoteIncident remoteIncident, List<RemoteDataMapping> newIncidentMappings, List<RemoteDataMapping> newReleaseMappings, List<RemoteDataMapping> oldReleaseMappings, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteCustomProperty[] incidentCustomProperties, RemoteDataMapping[] requirementMappings, RemoteDataMapping[] incidentMappings, RemoteDataMapping[] taskMappings, Project tfsProject, WorkItemStore workItemStore, string productName, RemoteDataMapping[] incidentSeverityMappings, RemoteDataMapping[] incidentPriorityMappings, RemoteDataMapping[] incidentStatusMappings, RemoteDataMapping[] incidentTypeMappings, RemoteDataMapping[] userMappings, RemoteDataMapping[] releaseMappings, TfsTeamProjectCollection tfsTeamProjectCollection)
        {
            //Get certain incident fields into local variables (if used more than once)
            int incidentId = remoteIncident.IncidentId.Value;
            int incidentStatusId = remoteIncident.IncidentStatusId.Value;
            RemoteDataMapping dataMapping;

            //Make sure we've not already loaded this incident
            if (InternalFunctions.FindMappingByInternalId(projectId, incidentId, incidentMappings) == null)
            {
                //Now get the work item type from the mapping
                //Tasks are handled separately unless they are mapped, need to check
                dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.IncidentTypeId.Value, incidentTypeMappings);
                if (dataMapping == null)
                {
                    //We can't find the matching item so log and move to the next incident
                    eventLog.WriteEntry("Unable to locate mapping entry for incident type " + remoteIncident.IncidentTypeId.Value + " in project " + projectId, EventLogEntryType.Error);
                    return;
                }
                string workItemTypeName = dataMapping.ExternalKey;

                //First we need to get the Iteration, mapped from the SpiraTest Release, if not create it
                //Need to do this before creating the work item as we may need to reload the project reference
                int iterationId = -1;
                if (remoteIncident.DetectedReleaseId.HasValue)
                {
                    int detectedReleaseId = remoteIncident.DetectedReleaseId.Value;
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, detectedReleaseId, releaseMappings);
                    if (dataMapping == null)
                    {
                        //Now check to see if recently added
                        dataMapping = InternalFunctions.FindMappingByInternalId(projectId, detectedReleaseId, newReleaseMappings.ToArray());
                    }
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so need to create a new iteration in TFS and add to mappings
                        LogTraceEvent(eventLog, "Adding new iteration in TFS for release " + detectedReleaseId + "\n", EventLogEntryType.Information);
                        Node newIterationNode = AddNewTfsIteration(tfsTeamProjectCollection, ref workItemStore, ref tfsProject, remoteIncident.DetectedReleaseVersionNumber);

                        //Add a new mapping entry if successful
                        if (newIterationNode != null)
                        {
                            SpiraImportExport.RemoteDataMapping newReleaseMapping = new SpiraImportExport.RemoteDataMapping();
                            newReleaseMapping.ProjectId = projectId;
                            newReleaseMapping.InternalId = detectedReleaseId;
                            newReleaseMapping.ExternalKey = newIterationNode.Id.ToString();
                            newReleaseMappings.Add(newReleaseMapping);
                            iterationId = newIterationNode.Id;
                        }
                    }
                    else
                    {
                        if (!Int32.TryParse(dataMapping.ExternalKey, out iterationId))
                        {
                            iterationId = -1;
                            eventLog.WriteEntry("The release/iteration external key " + dataMapping.ExternalKey + " in project " + projectId + " is invalid - it needs to be numeric!", EventLogEntryType.Warning);
                        }
                    }
                }

                //Now, create the new TFS work item and populate the standard fields that don't need mapping
                if (!tfsProject.WorkItemTypes.Contains(workItemTypeName))
                {
                    eventLog.WriteEntry("Unable to locate TFS work item type '" + workItemTypeName + "' in TFS project '" + tfsProject.Name + "'", EventLogEntryType.Error);
                    return;
                }

                WorkItemType workItemType = tfsProject.WorkItemTypes[workItemTypeName];
                WorkItem workItem = new WorkItem(workItemType);
                workItem.Title = remoteIncident.Name;
                //See if this work item type supports the rich-text 'steps to reproduce' or description fields
                if (workItemType.FieldDefinitions.Contains(TFS_FIELD_STEPS_TO_REPRODUCE))
                {
                    workItem[TFS_FIELD_STEPS_TO_REPRODUCE] = remoteIncident.Description;
                }
                if (workItemType.FieldDefinitions.Contains(TFS_FIELD_DESCRIPTION_RICH_TEXT))
                {
                    workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT] = remoteIncident.Description;
                }
                //The description field only supports plain text
                workItem.Description = InternalFunctions.HtmlRenderAsPlainText(remoteIncident.Description);

                if (iterationId != -1)
                {
                    workItem.IterationId = iterationId;
                }

                //Add a link to the SpiraTest incident
                string incidentUrl = spiraImportExport.System_GetWebServerUrl() + "/IncidentDetails.aspx?incidentId=" + incidentId.ToString();
                workItem.Links.Add(new Hyperlink(incidentUrl));

                //See if we need to populate TFS custom fields with the Spira incident ID and/or detector's name
                if (!String.IsNullOrEmpty(this.artifactIdTfsField) && workItem.Type.FieldDefinitions.Contains(this.artifactIdTfsField))
                {
                    workItem[this.artifactIdTfsField] = Constants.INCIDENT_PREFIX + remoteIncident.IncidentId.Value;
                }
                if (!String.IsNullOrEmpty(this.incidentDetectorTfsField) && workItem.Type.FieldDefinitions.Contains(this.incidentDetectorTfsField))
                {
                    workItem[this.incidentDetectorTfsField] = remoteIncident.OpenerName;
                }

                //We have to always initially create the work items in their default state
                //So changes to the State + Reason have to come later

                //Now get the incident status from the mapping
                dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.IncidentStatusId.Value, incidentStatusMappings);
                if (dataMapping == null)
                {
                    //We can't find the matching item so log and move to the next incident
                    eventLog.WriteEntry("Unable to locate mapping entry for incident status " + remoteIncident.IncidentStatusId + " in project " + projectId, EventLogEntryType.Error);
                    return;
                }
                //The status in SpiraTest = MSTFS State+Reason
                string[] stateAndReason = dataMapping.ExternalKey.Split('+');
                string tfsState = stateAndReason[0];
                string tfsReason = stateAndReason[1];

                //Now get the incident priority from the mapping (if priority is set)
                if (remoteIncident.PriorityId.HasValue)
                {
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.PriorityId.Value, incidentPriorityMappings);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so log and just don't set the priority
                        eventLog.WriteEntry("Unable to locate mapping entry for incident priority " + remoteIncident.PriorityId.Value + " in project " + projectId, EventLogEntryType.Warning);
                    }
                    else
                    {
                        if (workItemType.FieldDefinitions.Contains(TFS_FIELD_PRIORITY))
                        {
                            workItem[TFS_FIELD_PRIORITY] = dataMapping.ExternalKey;
                        }
                    }
                }

                //Now get the incident severity from the mapping (if severity is set)
                if (remoteIncident.SeverityId.HasValue)
                {
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.SeverityId.Value, incidentSeverityMappings);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so log and just don't set the severity
                        eventLog.WriteEntry("Unable to locate mapping entry for incident severity " + remoteIncident.SeverityId.Value + " in project " + projectId, EventLogEntryType.Warning);
                    }
                    else
                    {
                        if (workItemType.FieldDefinitions.Contains(TFS_FIELD_SEVERITY))
                        {
                            workItem[TFS_FIELD_SEVERITY] = dataMapping.ExternalKey;
                        }
                    }
                }

                //See if the creator is allowed to be set on the work-item
                if (workItemType.FieldDefinitions[CoreField.CreatedBy].IsEditable)
                {
                    dataMapping = FindUserMappingByInternalId(remoteIncident.OpenerId.Value, userMappings, spiraImportExport);
                    if (dataMapping == null)
                    {
                        //We can't find the matching user so ignore
                        eventLog.WriteEntry("Unable to locate mapping entry for user id " + remoteIncident.OpenerId.Value + " so leaving blank", EventLogEntryType.Warning);
                    }
                    else
                    {
                        workItem[CoreField.CreatedBy] = dataMapping.ExternalKey;
                    }
                }

                //Now set the assignee
                if (remoteIncident.OwnerId.HasValue)
                {
                    dataMapping = FindUserMappingByInternalId(remoteIncident.OwnerId.Value, userMappings, spiraImportExport);
                    if (dataMapping == null)
                    {
                        //We can't find the matching user so ignore
                        eventLog.WriteEntry("Unable to locate mapping entry for user id " + remoteIncident.OwnerId.Value + " so leaving blank", EventLogEntryType.Warning);
                    }
                    else
                    {
                        workItem[CoreField.AssignedTo] = dataMapping.ExternalKey;
                    }
                }

                //Now iterate through the incident custom properties
                ProcessIncidentCustomProperties(productName, projectId, remoteIncident, workItem, customPropertyMappingList, customPropertyValueMappingList, userMappings, spiraImportExport);

                //Validate the work item
                StringBuilder messages = new StringBuilder();
                if (ValidateItem(workItem, messages))
                {
                    //Finally save the new work item
                    try
                    {
                        workItem.Save();
                    }
                    catch (Exception exception)
                    {
                        //If we have TFS error TF26201
                        //Iterate through the fields to see which ones caused the error
                        if (exception.Message.Contains("TF26201"))
                        {
                            bool foundInvalidField = false;
                            foreach (Field field in workItem.Fields)
                            {
                                if (!field.IsValid)
                                {
                                    //Log the name of the field
                                    foundInvalidField = true;
                                    eventLog.WriteEntry("Error Adding " + productName + " Incident to Team Foundation Server because of bad field '" + field.Name + "' (" + exception.Message + ")", EventLogEntryType.Error);
                                }
                            }
                            if (!foundInvalidField)
                            {
                                //Log a general exception
                                eventLog.WriteEntry("Error Adding " + productName + " Incident to Team Foundation Server: " + exception.Message, EventLogEntryType.Error);
                            }
                        }
                        else
                        {
                            throw exception;
                        }
                    }

                    //Now we need to update the state and reasons to the final status
                    if (workItem.State != tfsState || workItem.Reason != tfsReason)
                    {
                        workItem.State = tfsState;
                        workItem.Reason = tfsReason;
                        if (ValidateItem(workItem, messages))
                        {
                            try
                            {
                                workItem.Save();
                            }
                            catch (Exception exception2)
                            {
                                //Need to catch this so that we can capture the ID and set the mappings
                                //otherwise duplicate items get created in TFS (!)
                                eventLog.WriteEntry("Error Setting Status+Reason (" + tfsState + ":" + tfsReason + ") in Team Foundation Server: " + exception2.Message, EventLogEntryType.Error);
                            }
                        }
                        else
                        {
                            //Log the detailed message as a warning because in this case we have managed to add the item already
                            //just with the default state+reason.
                            eventLog.WriteEntry("Warning Adding " + productName + " Incident to Team Foundation Server: " + messages.ToString(), EventLogEntryType.Warning);
                        }
                    }

                    //Extract the TFS Work Item ID and add to mappings table
                    SpiraImportExport.RemoteDataMapping newIncidentMapping = new SpiraImportExport.RemoteDataMapping();
                    newIncidentMapping.ProjectId = projectId;
                    newIncidentMapping.InternalId = incidentId;
                    newIncidentMapping.ExternalKey = workItem.Id.ToString();
                    newIncidentMappings.Add(newIncidentMapping);

                    //Finally add any comments as history items to the work item if appropriate
                    RemoteComment[] remoteComments = spiraImportExport.Incident_RetrieveComments(incidentId);
                    foreach (RemoteComment remoteComment in remoteComments)
                    {
                        workItem.History = remoteComment.Text;
                        workItem.Save();
                    }

                    //See if the Spira incident had any attachments
                    RemoteSort attachmentSort = new RemoteSort();
                    attachmentSort.SortAscending = true;
                    attachmentSort.PropertyName = "AttachmentId";
                    RemoteDocument[] remoteDocuments = remoteDocuments = spiraImportExport.Document_RetrieveForArtifact((int)Constants.ArtifactType.Incident, incidentId, null, attachmentSort);

                    //See if this incident has any associations
                    RemoteSort associationSort = new RemoteSort();
                    associationSort.SortAscending = true;
                    associationSort.PropertyName = "CreationDate";
                    RemoteAssociation[] remoteAssociations = spiraImportExport.Association_RetrieveForArtifact((int)Constants.ArtifactType.Incident, incidentId, null, associationSort);

                    //Add attachments to the work item if appropriate
                    if (remoteDocuments != null)
                    {
                        LogTraceEvent(String.Format("{0} incident has {1} document attachments to add to new TFS work item", productName, remoteDocuments.Length), EventLogEntryType.Information);
                        foreach (RemoteDocument remoteDocument in remoteDocuments)
                        {
                            LogTraceEvent(String.Format("Found {0} attachment '{1}'", productName, remoteDocument.FilenameOrUrl), EventLogEntryType.Information);

                            //See if we have a file attachment or simple URL
                            if (remoteDocument.AttachmentTypeId == (int)Constants.AttachmentType.File)
                            {
                                try
                                {
                                    //Get the binary data for the attachment
                                    byte[] binaryData = spiraImportExport.Document_OpenFile(remoteDocument.AttachmentId.Value);
                                    if (binaryData != null && binaryData.Length > 0)
                                    {
                                        LogTraceEvent(String.Format("Adding {0} attachment '{1}' to TFS work item", productName, remoteDocument.FilenameOrUrl), EventLogEntryType.Information);

                                        //First we need to write this file to a tempoary local path
                                        string filename = remoteDocument.FilenameOrUrl;
                                        string filepath = Path.Combine(System.Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.DoNotVerify), filename);
                                        using (FileStream writer = new FileStream(filepath, FileMode.Create))
                                        {
                                            writer.Write(binaryData, 0, binaryData.Length);
                                            writer.Close();
                                        }
                                        Attachment tfsAttachment = new Attachment(filepath, remoteDocument.Description);
                                        workItem.Attachments.Add(tfsAttachment);
                                        workItem.Save();

                                        //Clean up by deleting the file
                                        if (File.Exists(filepath))
                                        {
                                            File.Delete(filepath);
                                        }
                                    }
                                }
                                catch (Exception exception)
                                {
                                    //Log an error and continue because this can fail if the files are too large
                                    LogErrorEvent("Error adding " + productName + " incident attachment DC" + remoteDocument.AttachmentId.Value + " to JIRA: " + exception.Message + "\n. (The issue itself was added.)\n Stack Trace: " + exception.StackTrace, EventLogEntryType.Error);
                                }
                            }
                            if (remoteDocument.AttachmentTypeId == (int)Constants.AttachmentType.URL)
                            {
                                try
                                {
                                    LogTraceEvent(String.Format("Adding {0} URL attachment '{1}' to TFS work item", productName, remoteDocument.FilenameOrUrl), EventLogEntryType.Information);

                                    //Add as a web link
                                    string url = remoteDocument.FilenameOrUrl;
                                    Hyperlink hyperlink = new Hyperlink(url);
                                    hyperlink.Comment = remoteDocument.Description;
                                    workItem.Links.Add(hyperlink);
                                    workItem.Save();
                                }
                                catch (Exception exception)
                                {
                                    //Log an error and continue because this can fail if the files are too large
                                    LogErrorEvent("Error adding " + productName + " incident attachment DC" + remoteDocument.AttachmentId.Value + " to TFS: " + exception.Message + "\n. (The issue itself was added.)\n Stack Trace: " + exception.StackTrace, EventLogEntryType.Error);
                                }
                            }
                        }
                    }

                    //Add any artifact associations to this work item if appropriate
                    if (remoteAssociations != null)
                    {
                        LogTraceEvent(String.Format("{0} incident has {1} associations to add to new TFS work item", productName, remoteAssociations.Length), EventLogEntryType.Information);
                        foreach (RemoteAssociation remoteAssociation in remoteAssociations)
                        {
                            int? destWorkItemId = null;

                            if (remoteAssociation.DestArtifactTypeId == (int)Constants.ArtifactType.Requirement)
                            {
                                dataMapping = InternalFunctions.FindMappingByInternalId(remoteAssociation.DestArtifactId, requirementMappings);
                                if (dataMapping != null)
                                {
                                    destWorkItemId = Int32.Parse(dataMapping.ExternalKey);
                                }
                            }
                            if (remoteAssociation.DestArtifactTypeId == (int)Constants.ArtifactType.Incident)
                            {
                                dataMapping = InternalFunctions.FindMappingByInternalId(remoteAssociation.DestArtifactId, incidentMappings);
                                if (dataMapping != null)
                                {
                                    destWorkItemId = Int32.Parse(dataMapping.ExternalKey);
                                }
                            }
                            if (remoteAssociation.DestArtifactTypeId == (int)Constants.ArtifactType.Task)
                            {
                                dataMapping = InternalFunctions.FindMappingByInternalId(remoteAssociation.DestArtifactId, taskMappings);
                                if (dataMapping != null)
                                {
                                    destWorkItemId = Int32.Parse(dataMapping.ExternalKey);
                                }
                            }

                            //Create the link if a destination match was found
                            if (destWorkItemId.HasValue)
                            {
                                LogTraceEvent(String.Format("Adding work item link from {0} to {1}", workItem.Id, destWorkItemId.Value), EventLogEntryType.Information);

                                WorkItemLinkTypeEnd linkTypeEnd = workItemStore.WorkItemLinkTypes["System.LinkTypes.Related"].ReverseEnd;
                                WorkItemLink workItemLink = new WorkItemLink(linkTypeEnd, destWorkItemId.Value);
                                workItemLink.Comment = remoteAssociation.Comment;
                                workItem.Links.Add(workItemLink);
                                workItem.Save();
                            }
                        }
                    }
                }
                else
                {
                    //Log the detailed error message
                    eventLog.WriteEntry("Error Adding " + productName + " Incident to Team Foundation Server: " + messages.ToString(), EventLogEntryType.Error);
                }
            }
        }

        /// <summary>
        /// Processes a new TFS work item as a Requirement
        /// </summary>
        private void ProcessNewWorkItemAsRequirement(int projectId, ImportExportClient spiraImportExport, WorkItem workItem, List<RemoteDataMapping> newRequirementMappings, List<RemoteDataMapping> newReleaseMappings, List<RemoteDataMapping> oldReleaseMappings, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteCustomProperty[] requirementCustomProperties, RemoteDataMapping[] requirementMappings, RemoteDataMapping[] incidentMappings, RemoteDataMapping[] taskMappings, Project tfsProject, WorkItemStore workItemStore, string productName, RemoteDataMapping[] priorityMappings, RemoteDataMapping[] statusMappings, RemoteDataMapping[] userMappings, RemoteDataMapping[] releaseMappings)
        {
            //Make sure it has not been already mapped
            if (InternalFunctions.FindMappingByExternalKey(projectId, workItem.Id.ToString(), requirementMappings, false) != null)
            {
                return;
            }

            //We need to add a new requirement to SpiraTeam
            RemoteRequirement remoteRequirement = new RemoteRequirement();
            remoteRequirement.ProjectId = projectId;

            //Update the requirement with the text fields
            if (!String.IsNullOrEmpty(workItem.Title))
            {
                remoteRequirement.Name = workItem.Title;
            }
            //See if we're using a rich text or plain text description field
            if (workItem.Type.FieldDefinitions.Contains(TFS_FIELD_DESCRIPTION_RICH_TEXT) && !String.IsNullOrEmpty(workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT].ToString()))
            {
                remoteRequirement.Description = (string)workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT];
            }
            else
            {
                if (String.IsNullOrEmpty(workItem.Description))
                {
                    remoteRequirement.Description = "Empty Description in TFS";
                }
                else
                {
                    remoteRequirement.Description = workItem.Description;
                }
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the requirement name and description\n", EventLogEntryType.Information);

            //Now get the requirement status from the State mapping
            RemoteDataMapping dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.State, statusMappings, true);
            if (dataMapping == null)
            {
                //We can't find the matching item so log and ignore
                eventLog.WriteEntry("Unable to locate mapping entry for Requirement State " + workItem.State + " in project " + projectId, EventLogEntryType.Error);
            }
            else
            {
                remoteRequirement.StatusId = dataMapping.InternalId;
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the requirement status\n", EventLogEntryType.Information);

            //Importance
            //Now get the work item priority from the mapping (if priority is set)
            if (workItem.Fields.Contains("Priority") && workItem.Fields["Priority"].IsValid)
            {
                if (String.IsNullOrEmpty(workItem["Priority"].ToString()))
                {
                    remoteRequirement.ImportanceId = null;
                }
                else
                {
                    dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem["Priority"].ToString(), priorityMappings, true);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so log and just don't set the priority
                        eventLog.WriteEntry("Unable to locate mapping entry for work item priority " + workItem["Priority"].ToString() + " in project " + projectId, EventLogEntryType.Warning);
                    }
                    else
                    {
                        remoteRequirement.ImportanceId = dataMapping.InternalId;
                    }
                }
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the requirement importance\n", EventLogEntryType.Information);

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the requirement dates\n", EventLogEntryType.Information);

            //Update the estimated effort
            if (workItem.Fields.Contains(TFS_FIELD_COMPLETED_WORK) && workItem[TFS_FIELD_COMPLETED_WORK] != null)
            {
                double completedWorkHours = (double)workItem[TFS_FIELD_COMPLETED_WORK];
                int actualEffortMins = (int)(completedWorkHours * (double)60);
                remoteRequirement.PlannedEffort = actualEffortMins;
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the requirement work\n", EventLogEntryType.Information);

            //Now we need to see if any of the SpiraTest custom properties that map to TFS fields have changed in TFS
            ProcessWorkItemCustomFieldChanges(projectId, workItem, remoteRequirement, requirementCustomProperties, customPropertyMappingList, customPropertyValueMappingList, userMappings, spiraImportExport);

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the requirement custom properties\n", EventLogEntryType.Information);

            //Set the requirement author
            if (!String.IsNullOrEmpty((string)workItem[CoreField.CreatedBy]))
            {
                dataMapping = FindUserMappingByExternalKey((string)workItem[CoreField.CreatedBy], userMappings, spiraImportExport);
                if (dataMapping == null)
                {
                    //We can't find the matching user so log and ignore
                    eventLog.WriteEntry("Unable to locate mapping entry for TFS user " + (string)workItem[CoreField.CreatedBy] + " so using the synchronization user", EventLogEntryType.Warning);
                }
                else
                {
                    remoteRequirement.AuthorId = dataMapping.InternalId;
                    LogTraceEvent(eventLog, "Got the author " + remoteRequirement.AuthorId.ToString() + "\n", EventLogEntryType.Information);
                }
            }

            //Set the owner/assignee
            if (String.IsNullOrEmpty((string)workItem[CoreField.AssignedTo]))
            {
                remoteRequirement.OwnerId = null;
            }
            else
            {
                dataMapping = FindUserMappingByExternalKey((string)workItem[CoreField.AssignedTo], userMappings, spiraImportExport);
                if (dataMapping == null)
                {
                    //We can't find the matching user so log and ignore
                    eventLog.WriteEntry("Unable to locate mapping entry for TFS user " + (string)workItem[CoreField.AssignedTo] + " so ignoring the assignee change", EventLogEntryType.Error);
                }
                else
                {
                    remoteRequirement.OwnerId = dataMapping.InternalId;
                    LogTraceEvent(eventLog, "Got the assignee " + remoteRequirement.OwnerId.ToString() + "\n", EventLogEntryType.Information);
                }
            }

            //Specify the requirement release if applicable
            if (!String.IsNullOrEmpty(workItem.IterationPath))
            {
                //See if we have a mapped SpiraTest release
                dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), releaseMappings, false);
                if (dataMapping == null)
                {
                    //Now check to see if recently added
                    dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), newReleaseMappings.ToArray(), false);
                }
                if (dataMapping == null)
                {
                    //We can't find the matching item so need to create a new release in SpiraTest and add to mappings

                    //Need to iterate through the TFS iteration node tree to get the full node object
                    Node iterationNode = GetMatchingNode(tfsProject.IterationRootNodes, workItem.IterationId);
                    if (iterationNode != null)
                    {
                        LogTraceEvent(eventLog, "Adding new release in " + productName + " for iteration " + iterationNode.Name + "\n", EventLogEntryType.Information);
                        SpiraImportExport.RemoteRelease remoteRelease = new SpiraImportExport.RemoteRelease();
                        remoteRelease.Name = iterationNode.Name;
                        remoteRelease.VersionNumber = "TFS-" + iterationNode.Id;
                        remoteRelease.CreatorId = 1;    //System Administrator
                        remoteRelease.Active = true;
                        remoteRelease.StartDate = DateTime.Now.Date;
                        remoteRelease.EndDate = DateTime.Now.Date.AddDays(5);
                        remoteRelease.CreationDate = DateTime.Now;
                        remoteRelease.ResourceCount = 1;
                        remoteRelease.DaysNonWorking = 0;
                        remoteRelease = spiraImportExport.Release_Create(remoteRelease, null);

                        //Add a new mapping entry
                        SpiraImportExport.RemoteDataMapping newReleaseMapping = new SpiraImportExport.RemoteDataMapping();
                        newReleaseMapping.ProjectId = projectId;
                        newReleaseMapping.InternalId = remoteRelease.ReleaseId.Value;
                        newReleaseMapping.ExternalKey = iterationNode.Id.ToString();
                        newReleaseMappings.Add(newReleaseMapping);
                        remoteRequirement.ReleaseId = newReleaseMapping.InternalId;
                    }
                }
                else
                {
                    remoteRequirement.ReleaseId = dataMapping.InternalId;
                }
            }

            //Finally create the requirement in SpiraTest, exceptions get logged
            int requirementId = spiraImportExport.Requirement_Create2(remoteRequirement, null).RequirementId.Value;
            SpiraImportExport.RemoteDataMapping newRequirementMapping = new SpiraImportExport.RemoteDataMapping();
            newRequirementMapping.ProjectId = projectId;
            newRequirementMapping.InternalId = requirementId;
            newRequirementMapping.ExternalKey = workItem.Id.ToString();
            newRequirementMappings.Add(newRequirementMapping);

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Successfully created new requirement in " + productName + "\n", EventLogEntryType.Information);

            //Now we need to get all the comments attached to the work item in TFS
            RevisionCollection revisions = workItem.Revisions;

            //Iterate through all the comments and add any to SpiraTest
            if (revisions != null)
            {
                foreach (Revision revision in revisions)
                {
                    //Add the author, date and body to the resolution
                    if (revision.Fields[CoreField.History].Value != null && revision.Fields[CoreField.History].Value.ToString() != "")
                    {
                        //Get the resolution author mapping
                        string revisionCreatedBy = (string)revision.Fields[CoreField.ChangedBy].Value;
                        LogTraceEvent(eventLog, "Looking for comments author: '" + revisionCreatedBy + "'\n", EventLogEntryType.Information);
                        int? creatorId = null;
                        dataMapping = FindUserMappingByExternalKey(revisionCreatedBy, userMappings, spiraImportExport);
                        if (dataMapping != null)
                        {
                            creatorId = dataMapping.InternalId;
                            LogTraceEvent(eventLog, "Got the resolution creator: " + creatorId.ToString() + "\n", EventLogEntryType.Information);
                        }

                        //Add the comment to SpiraTest
                        RemoteComment newComment = new RemoteComment();
                        newComment.ArtifactId = requirementId;
                        newComment.UserId = creatorId;
                        newComment.CreationDate = ((DateTime)revision.Fields[CoreField.ChangedDate].Value).ToUniversalTime();
                        newComment.Text = (string)revision.Fields[CoreField.History].Value;

                        spiraImportExport.Requirement_CreateComment(newComment);
                    }
                }
            }
            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the comments/history\n", EventLogEntryType.Information);

            //Next add the SpiraTeam requirement ID to TFS if appropriate
            if (!String.IsNullOrEmpty(this.artifactIdTfsField) && workItem.Type.FieldDefinitions.Contains(this.artifactIdTfsField))
            {
                workItem[this.artifactIdTfsField] = Constants.REQUIREMENT_PREFIX + requirementId;
                workItem.Save();
            }

            //If we have any TFS links, need to handle them
            if (workItem.Links != null && workItem.Links.Count > 0)
            {
                ProcessTfsWorkItemLinks(workItem.Links, spiraImportExport, requirementId, Constants.ArtifactType.Requirement, requirementMappings, incidentMappings, taskMappings);
            }

            //If we have any TFS attachments, need to handle them
            if (workItem.Attachments != null && workItem.Attachments.Count > 0)
            {
                ProcessTfsWorkItemAttachments(workItem.Attachments, spiraImportExport, requirementId, Constants.ArtifactType.Requirement);
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Added the requirement id to the TFS work item\n", EventLogEntryType.Information);
        }

        /// <summary>
        /// Processes a new TFS work item as a Task
        /// </summary>
        private void ProcessNewWorkItemAsTask(int projectId, ImportExportClient spiraImportExport, WorkItem workItem, List<RemoteDataMapping> newTaskMappings, List<RemoteDataMapping> newReleaseMappings, List<RemoteDataMapping> oldReleaseMappings, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteCustomProperty[] taskCustomProperties, RemoteDataMapping[] requirementMappings, RemoteDataMapping[] incidentMappings, RemoteDataMapping[] taskMappings, Project tfsProject, WorkItemStore workItemStore, string productName, RemoteDataMapping[] priorityMappings, RemoteDataMapping[] statusMappings, RemoteDataMapping[] userMappings, RemoteDataMapping[] releaseMappings)
        {
            //Make sure it has not been already mapped
            if (InternalFunctions.FindMappingByExternalKey(projectId, workItem.Id.ToString(), taskMappings, false) != null)
            {
                return;
            }

            //We need to add a new task to SpiraTeam
            RemoteTask remoteTask = new RemoteTask();
            remoteTask.ProjectId = projectId;

            //Update the task with the text fields
            if (!String.IsNullOrEmpty(workItem.Title))
            {
                remoteTask.Name = workItem.Title;
            }
            //See if we're using a rich text or plain text description field
            if (workItem.Type.FieldDefinitions.Contains(TFS_FIELD_DESCRIPTION_RICH_TEXT) && !String.IsNullOrEmpty(workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT].ToString()))
            {
                remoteTask.Description = (string)workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT];
            }
            else
            {
                if (String.IsNullOrEmpty(workItem.Description))
                {
                    remoteTask.Description = "Empty Description in TFS";
                }
                else
                {
                    remoteTask.Description = workItem.Description;
                }
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the task name and description\n", EventLogEntryType.Information);

            //Now get the task status from the State mapping
            RemoteDataMapping dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.State, statusMappings, true);
            if (dataMapping == null)
            {
                //We can't find the matching item so log and ignore
                eventLog.WriteEntry("Unable to locate mapping entry for Task State " + workItem.State + " in project " + projectId, EventLogEntryType.Error);
            }
            else
            {
                remoteTask.TaskStatusId = dataMapping.InternalId;
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the task status\n", EventLogEntryType.Information);

            //Now get the work item priority from the mapping (if priority is set)
            if (workItem.Fields.Contains("Priority") && workItem.Fields["Priority"].IsValid)
            {
                if (String.IsNullOrEmpty(workItem["Priority"].ToString()))
                {
                    remoteTask.TaskPriorityId = null;
                }
                else
                {
                    dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem["Priority"].ToString(), priorityMappings, true);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so log and just don't set the priority
                        eventLog.WriteEntry("Unable to locate mapping entry for work item priority " + workItem["Priority"].ToString() + " in project " + projectId, EventLogEntryType.Warning);
                    }
                    else
                    {
                        remoteTask.TaskPriorityId = dataMapping.InternalId;
                    }
                }
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the task priority\n", EventLogEntryType.Information);

            //Update the dates and efforts
            if (workItem[TFS_FIELD_START_DATE] != null)
            {
                remoteTask.StartDate = ((DateTime)workItem[TFS_FIELD_START_DATE]).ToUniversalTime();
            }
            if (workItem[TFS_FIELD_FINISH_DATE] != null)
            {
                remoteTask.EndDate = ((DateTime)workItem[TFS_FIELD_FINISH_DATE]).ToUniversalTime();
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the task dates\n", EventLogEntryType.Information);

            //Update the actual and estimated work
            if (workItem.Fields.Contains(TFS_FIELD_COMPLETED_WORK) && workItem[TFS_FIELD_COMPLETED_WORK] != null)
            {
                double completedWorkHours = (double)workItem[TFS_FIELD_COMPLETED_WORK];
                int actualEffortMins = (int)(completedWorkHours * (double)60);
                if (!remoteTask.EstimatedEffort.HasValue)
                {
                    remoteTask.EstimatedEffort = actualEffortMins;
                }
                remoteTask.ActualEffort = actualEffortMins;
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the task work\n", EventLogEntryType.Information);

            //Now we need to see if any of the SpiraTest custom properties that map to TFS fields have changed in TFS
            ProcessWorkItemCustomFieldChanges(projectId, workItem, remoteTask, taskCustomProperties, customPropertyMappingList, customPropertyValueMappingList, userMappings, spiraImportExport);

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the task custom properties\n", EventLogEntryType.Information);

            //Set the task creator
            if (!String.IsNullOrEmpty((string)workItem[CoreField.CreatedBy]))
            {
                dataMapping = FindUserMappingByExternalKey((string)workItem[CoreField.CreatedBy], userMappings, spiraImportExport);
                if (dataMapping == null)
                {
                    //We can't find the matching user so log and ignore
                    eventLog.WriteEntry("Unable to locate mapping entry for TFS user " + (string)workItem[CoreField.CreatedBy] + " so using the synchronization user", EventLogEntryType.Warning);
                }
                else
                {
                    remoteTask.CreatorId = dataMapping.InternalId;
                    LogTraceEvent(eventLog, "Got the creator " + remoteTask.CreatorId.ToString() + "\n", EventLogEntryType.Information);
                }
            }

            //Set the owner/assignee
            if (String.IsNullOrEmpty((string)workItem[CoreField.AssignedTo]))
            {
                remoteTask.OwnerId = null;
            }
            else
            {
                dataMapping = FindUserMappingByExternalKey((string)workItem[CoreField.AssignedTo], userMappings, spiraImportExport);
                if (dataMapping == null)
                {
                    //We can't find the matching user so log and ignore
                    eventLog.WriteEntry("Unable to locate mapping entry for TFS user " + (string)workItem[CoreField.AssignedTo] + " so ignoring the assignee change", EventLogEntryType.Error);
                }
                else
                {
                    remoteTask.OwnerId = dataMapping.InternalId;
                    LogTraceEvent(eventLog, "Got the assignee " + remoteTask.OwnerId.ToString() + "\n", EventLogEntryType.Information);
                }
            }

            //Specify the task release if applicable
            if (!String.IsNullOrEmpty(workItem.IterationPath))
            {
                //See if we have a mapped SpiraTest release
                dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), releaseMappings, false);
                if (dataMapping == null)
                {
                    //Now check to see if recently added
                    dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), newReleaseMappings.ToArray(), false);
                }
                if (dataMapping == null)
                {
                    //We can't find the matching item so need to create a new release in SpiraTest and add to mappings

                    //Need to iterate through the TFS iteration node tree to get the full node object
                    Node iterationNode = GetMatchingNode(tfsProject.IterationRootNodes, workItem.IterationId);
                    if (iterationNode != null)
                    {
                        LogTraceEvent(eventLog, "Adding new release in " + productName + " for iteration " + iterationNode.Name + "\n", EventLogEntryType.Information);
                        SpiraImportExport.RemoteRelease remoteRelease = new SpiraImportExport.RemoteRelease();
                        remoteRelease.Name = iterationNode.Name;
                        remoteRelease.VersionNumber = "TFS-" + iterationNode.Id;
                        remoteRelease.CreatorId = 1;    //System Administrator
                        remoteRelease.Active = true;
                        remoteRelease.StartDate = DateTime.Now.Date;
                        remoteRelease.EndDate = DateTime.Now.Date.AddDays(5);
                        remoteRelease.CreationDate = DateTime.Now;
                        remoteRelease.ResourceCount = 1;
                        remoteRelease.DaysNonWorking = 0;
                        remoteRelease = spiraImportExport.Release_Create(remoteRelease, null);

                        //Add a new mapping entry
                        SpiraImportExport.RemoteDataMapping newReleaseMapping = new SpiraImportExport.RemoteDataMapping();
                        newReleaseMapping.ProjectId = projectId;
                        newReleaseMapping.InternalId = remoteRelease.ReleaseId.Value;
                        newReleaseMapping.ExternalKey = iterationNode.Id.ToString();
                        newReleaseMappings.Add(newReleaseMapping);
                        remoteTask.ReleaseId = newReleaseMapping.InternalId;
                    }
                }
                else
                {
                    remoteTask.ReleaseId = dataMapping.InternalId;
                }
            }

            //Finally create the task in SpiraTest, exceptions get logged
            int taskId = spiraImportExport.Task_Create(remoteTask).TaskId.Value;
            SpiraImportExport.RemoteDataMapping newTaskMapping = new SpiraImportExport.RemoteDataMapping();
            newTaskMapping.ProjectId = projectId;
            newTaskMapping.InternalId = taskId;
            newTaskMapping.ExternalKey = workItem.Id.ToString();
            newTaskMappings.Add(newTaskMapping);

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Successfully created new task in " + productName + "\n", EventLogEntryType.Information);

            //Now we need to get all the comments attached to the work item in TFS
            RevisionCollection revisions = workItem.Revisions;

            //Iterate through all the comments and add any to SpiraTest
            if (revisions != null)
            {
                foreach (Revision revision in revisions)
                {
                    //Add the author, date and body to the resolution
                    if (revision.Fields[CoreField.History].Value != null && revision.Fields[CoreField.History].Value.ToString() != "")
                    {
                        //Get the resolution author mapping
                        string revisionCreatedBy = (string)revision.Fields[CoreField.ChangedBy].Value;
                        LogTraceEvent(eventLog, "Looking for comments author: '" + revisionCreatedBy + "'\n", EventLogEntryType.Information);
                        int? creatorId = null;
                        dataMapping = FindUserMappingByExternalKey(revisionCreatedBy, userMappings, spiraImportExport);
                        if (dataMapping != null)
                        {
                            creatorId = dataMapping.InternalId;
                            LogTraceEvent(eventLog, "Got the resolution creator: " + creatorId.ToString() + "\n", EventLogEntryType.Information);
                        }

                        //Add the comment to SpiraTest
                        RemoteComment newComment = new RemoteComment();
                        newComment.ArtifactId = taskId;
                        newComment.UserId = creatorId;
                        newComment.CreationDate = ((DateTime)revision.Fields[CoreField.ChangedDate].Value).ToUniversalTime();
                        newComment.Text = (string)revision.Fields[CoreField.History].Value;

                        spiraImportExport.Task_CreateComment(newComment);
                    }
                }
            }
            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the comments/history\n", EventLogEntryType.Information);

            //Next add the SpiraTeam task ID to TFS if appropriate
            if (!String.IsNullOrEmpty(this.artifactIdTfsField) && workItem.Type.FieldDefinitions.Contains(this.artifactIdTfsField))
            {
                workItem[this.artifactIdTfsField] = Constants.TASK_PREFIX + taskId;
                workItem.Save();
            }

            //If we have any TFS links, need to handle them
            if (workItem.Links != null && workItem.Links.Count > 0)
            {
                ProcessTfsWorkItemLinks(workItem.Links, spiraImportExport, taskId, Constants.ArtifactType.Task, requirementMappings, incidentMappings, taskMappings);
            }

            //If we have any TFS attachments, need to handle them
            if (workItem.Attachments != null && workItem.Attachments.Count > 0)
            {
                ProcessTfsWorkItemAttachments(workItem.Attachments, spiraImportExport, taskId, Constants.ArtifactType.Task);
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Added the task id to the TFS work item\n", EventLogEntryType.Information);
        }

        /// <summary>
        /// Processes a new TFS work item as an Incident
        /// </summary>
        private void ProcessNewWorkItemAsIncident(int projectId, ImportExportClient spiraImportExport, WorkItem workItem, List<RemoteDataMapping> newIncidentMappings, List<RemoteDataMapping> newReleaseMappings, List<RemoteDataMapping> oldReleaseMappings, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteCustomProperty[] incidentCustomProperties, RemoteDataMapping[] requirementMappings, RemoteDataMapping[] incidentMappings, RemoteDataMapping[] taskMappings, Project tfsProject, WorkItemStore workItemStore, string productName, RemoteDataMapping[] incidentSeverityMappings, RemoteDataMapping[] incidentPriorityMappings, RemoteDataMapping[] incidentStatusMappings, RemoteDataMapping[] incidentTypeMappings, RemoteDataMapping[] userMappings, RemoteDataMapping[] releaseMappings)
        {
            //Make sure it has not been already mapped
            if (InternalFunctions.FindMappingByExternalKey(projectId, workItem.Id.ToString(), incidentMappings, false) != null)
            {
                return;
            }

            //We need to add a new incident to SpiraTeam
            RemoteIncident remoteIncident = new RemoteIncident();
            remoteIncident.ProjectId = projectId;

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Retrieved incident in " + productName + "\n", EventLogEntryType.Information);

            //Update the incident with the text fields
            if (!String.IsNullOrEmpty(workItem.Title))
            {
                remoteIncident.Name = workItem.Title;
            }
            //See if we're using the plain-text description field or the rich-text
            //steps to reproduce field
            if (workItem.Type.FieldDefinitions.Contains(TFS_FIELD_STEPS_TO_REPRODUCE) && !String.IsNullOrEmpty(workItem[TFS_FIELD_STEPS_TO_REPRODUCE].ToString()))
            {
                remoteIncident.Description = (string)workItem[TFS_FIELD_STEPS_TO_REPRODUCE];
            }
            else if (workItem.Type.FieldDefinitions.Contains(TFS_FIELD_DESCRIPTION_RICH_TEXT) && !String.IsNullOrEmpty(workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT].ToString()))
            {
                remoteIncident.Description = (string)workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT];
            }
            else
            {
                if (String.IsNullOrEmpty(workItem.Description))
                {
                    remoteIncident.Description = "Empty Description in TFS";
                }
                else
                {
                    remoteIncident.Description = workItem.Description;
                }
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the incident name and description\n", EventLogEntryType.Information);

            //Get the type of the incident
            RemoteDataMapping dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.Type.Name, incidentTypeMappings, true);
            if (dataMapping == null)
            {
                //We can't find the matching item so log and just don't set the priority
                eventLog.WriteEntry("Unable to locate mapping entry for work item type " + workItem.Type.Name + " in project " + projectId, EventLogEntryType.Error);
                return;
            }
            else
            {
                remoteIncident.IncidentTypeId = dataMapping.InternalId;
            }

            //Now get the work item priority from the mapping (if priority is set)
            if (workItem.Fields.Contains("Priority") && workItem.Fields["Priority"].IsValid)
            {
                if (String.IsNullOrEmpty(workItem["Priority"].ToString()))
                {
                    remoteIncident.PriorityId = null;
                }
                else
                {
                    dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem["Priority"].ToString(), incidentPriorityMappings, true);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so log and just don't set the priority
                        eventLog.WriteEntry("Unable to locate mapping entry for work item priority " + workItem["Priority"].ToString() + " in project " + projectId, EventLogEntryType.Warning);
                    }
                    else
                    {
                        remoteIncident.PriorityId = dataMapping.InternalId;
                    }
                }
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the priority\n", EventLogEntryType.Information);

            //Now get the work item status from the State+Reason mapping
            string stateAndReason = workItem.State + "+" + workItem.Reason;
            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, stateAndReason, incidentStatusMappings, true);
            if (dataMapping == null)
            {
                //We can't find the matching item so log and ignore
                eventLog.WriteEntry("Unable to locate mapping entry for State+Reason " + stateAndReason + " in project " + projectId, EventLogEntryType.Error);
            }
            else
            {
                remoteIncident.IncidentStatusId = dataMapping.InternalId;
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the incident status\n", EventLogEntryType.Information);

            //Now we need to see if any of the SpiraTest custom properties that map to TFS fields have changed in TFS
            ProcessWorkItemCustomFieldChanges(projectId, workItem, remoteIncident, incidentCustomProperties, customPropertyMappingList, customPropertyValueMappingList, userMappings, spiraImportExport);

            if (!String.IsNullOrEmpty((string)workItem[CoreField.CreatedBy]))
            {
                dataMapping = FindUserMappingByExternalKey((string)workItem[CoreField.CreatedBy], userMappings, spiraImportExport);
                if (dataMapping == null)
                {
                    //We can't find the matching user so log and ignore
                    eventLog.WriteEntry("Unable to locate mapping entry for TFS user " + (string)workItem[CoreField.CreatedBy] + " so using the synchronization user", EventLogEntryType.Warning);
                }
                else
                {
                    remoteIncident.OpenerId = dataMapping.InternalId;
                    LogTraceEvent(eventLog, "Got the detector " + remoteIncident.OpenerId.ToString() + "\n", EventLogEntryType.Information);
                }
            }

            if (String.IsNullOrEmpty((string)workItem[CoreField.AssignedTo]))
            {
                remoteIncident.OwnerId = null;
            }
            else
            {
                dataMapping = FindUserMappingByExternalKey((string)workItem[CoreField.AssignedTo], userMappings, spiraImportExport);
                if (dataMapping == null)
                {
                    //We can't find the matching user so log and ignore
                    eventLog.WriteEntry("Unable to locate mapping entry for TFS user " + (string)workItem[CoreField.AssignedTo] + " so ignoring the assignee change", EventLogEntryType.Error);
                }
                else
                {
                    remoteIncident.OwnerId = dataMapping.InternalId;
                    LogTraceEvent(eventLog, "Got the assignee " + remoteIncident.OwnerId.ToString() + "\n", EventLogEntryType.Information);
                }
            }

            //Specify the resolved-in release if applicable
            if (!String.IsNullOrEmpty(workItem.IterationPath))
            {
                //See if we have a mapped SpiraTest release
                dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), releaseMappings, false);
                if (dataMapping == null)
                {
                    //Now check to see if recently added
                    dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), newReleaseMappings.ToArray(), false);
                }
                if (dataMapping == null)
                {
                    //We can't find the matching item so need to create a new release in SpiraTest and add to mappings

                    //Need to iterate through the TFS iteration node tree to get the full node object
                    Node iterationNode = GetMatchingNode(tfsProject.IterationRootNodes, workItem.IterationId);
                    if (iterationNode != null)
                    {
                        LogTraceEvent(eventLog, "Adding new release in " + productName + " for iteration " + iterationNode.Name + "\n", EventLogEntryType.Information);
                        SpiraImportExport.RemoteRelease remoteRelease = new SpiraImportExport.RemoteRelease();
                        remoteRelease.Name = iterationNode.Name;
                        remoteRelease.VersionNumber = "TFS-" + iterationNode.Id;
                        remoteRelease.CreatorId = remoteIncident.OpenerId;
                        remoteRelease.Active = true;
                        remoteRelease.StartDate = DateTime.Now.Date;
                        remoteRelease.EndDate = DateTime.Now.Date.AddDays(5);
                        remoteRelease.CreationDate = DateTime.Now;
                        remoteRelease.ResourceCount = 1;
                        remoteRelease.DaysNonWorking = 0;
                        remoteRelease = spiraImportExport.Release_Create(remoteRelease, null);

                        //Add a new mapping entry
                        SpiraImportExport.RemoteDataMapping newReleaseMapping = new SpiraImportExport.RemoteDataMapping();
                        newReleaseMapping.ProjectId = projectId;
                        newReleaseMapping.InternalId = remoteRelease.ReleaseId.Value;
                        newReleaseMapping.ExternalKey = iterationNode.Id.ToString();
                        newReleaseMappings.Add(newReleaseMapping);
                        remoteIncident.ResolvedReleaseId = newReleaseMapping.InternalId;
                    }
                }
                else
                {
                    remoteIncident.ResolvedReleaseId = dataMapping.InternalId;
                }
            }

            //Next add the incident to SpiraTeam and add to the mappings
            int incidentId = spiraImportExport.Incident_Create(remoteIncident).IncidentId.Value;
            SpiraImportExport.RemoteDataMapping newIncidentMapping = new SpiraImportExport.RemoteDataMapping();
            newIncidentMapping.ProjectId = projectId;
            newIncidentMapping.InternalId = incidentId;
            newIncidentMapping.ExternalKey = workItem.Id.ToString();
            newIncidentMappings.Add(newIncidentMapping);

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Successfully create new incident in " + productName + "\n", EventLogEntryType.Information);

            //Now we need to get all the comments attached to the work item in TFS
            RevisionCollection revisions = workItem.Revisions;

            //Iterate through all the comments and add any to SpiraTest
            List<RemoteComment> newIncidentComments = new List<RemoteComment>();
            if (revisions != null)
            {
                foreach (Revision revision in revisions)
                {
                    //Add the author, date and body to the resolution
                    if (revision.Fields[CoreField.History].Value != null && revision.Fields[CoreField.History].Value.ToString() != "")
                    {
                        //Get the resolution author mapping
                        string revisionCreatedBy = (string)revision.Fields[CoreField.ChangedBy].Value;
                        LogTraceEvent(eventLog, "Looking for comments author: '" + revisionCreatedBy + "'\n", EventLogEntryType.Information);
                        int? creatorId = null;
                        dataMapping = FindUserMappingByExternalKey(revisionCreatedBy, userMappings, spiraImportExport);
                        if (dataMapping != null)
                        {
                            creatorId = dataMapping.InternalId;
                            LogTraceEvent(eventLog, "Got the resolution creator: " + creatorId.ToString() + "\n", EventLogEntryType.Information);
                        }

                        //Add the comment to SpiraTest
                        RemoteComment newIncidentComment = new RemoteComment();
                        newIncidentComment.ArtifactId = incidentId;
                        newIncidentComment.UserId = creatorId;
                        newIncidentComment.CreationDate = ((DateTime)revision.Fields[CoreField.ChangedDate].Value).ToUniversalTime();
                        newIncidentComment.Text = (string)revision.Fields[CoreField.History].Value;
                        newIncidentComments.Add(newIncidentComment);
                    }
                }
            }
            spiraImportExport.Incident_AddComments(newIncidentComments.ToArray());

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the comments/history\n", EventLogEntryType.Information);

            //Next add the SpiraTeam incident ID to TFS if appropriate
            if (!String.IsNullOrEmpty(this.artifactIdTfsField) && workItem.Type.FieldDefinitions.Contains(this.artifactIdTfsField))
            {
                workItem[this.artifactIdTfsField] = Constants.INCIDENT_PREFIX + incidentId;
                workItem.Save();
            }

            //If we have any TFS links, need to handle them
            if (workItem.Links != null && workItem.Links.Count > 0)
            {
                ProcessTfsWorkItemLinks(workItem.Links, spiraImportExport, incidentId, Constants.ArtifactType.Incident, requirementMappings, incidentMappings, taskMappings);
            }

            //If we have any TFS attachments, need to handle them
            if (workItem.Attachments != null && workItem.Attachments.Count > 0)
            {
                ProcessTfsWorkItemAttachments(workItem.Attachments, spiraImportExport, incidentId, Constants.ArtifactType.Incident);
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Added the incident id to the TFS work item\n", EventLogEntryType.Information);
        }

        /// <summary>
        /// Processes a requirement that has been updated in TFS
        /// </summary>
        private void ProcessUpdatedRequirement(int projectId, ImportExportClient spiraImportExport, RemoteDataMapping requirementMapping, List<RemoteDataMapping> newReleaseMappings, List<RemoteDataMapping> oldReleaseMappings, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteCustomProperty[] requirementCustomProperties, Project tfsProject, WorkItemStore workItemStore, string productName, RemoteDataMapping[] priorityMappings, RemoteDataMapping[] statusMappings, RemoteDataMapping[] userMappings, RemoteDataMapping[] releaseMappings)
        {
            //Make sure it's for the current project
            if (requirementMapping.ProjectId != projectId)
            {
                LogTraceEvent(eventLog, "Projects didn't match, so aborting update", EventLogEntryType.Warning);
                return;
            }

            //First we need to retrieve the requirement from Spira
            int requirementId = requirementMapping.InternalId;
            int workItemId;
            if (!Int32.TryParse(requirementMapping.ExternalKey, out workItemId))
            {
                LogErrorEvent(String.Format("Unable to convert requirement external key '{0}' to integer, so ignoring.", requirementMapping.ExternalKey), EventLogEntryType.Warning);
                return;
            }
            RemoteRequirement remoteRequirement = spiraImportExport.Requirement_RetrieveById(requirementId);

            //Now retrieve the work item from MSTFS
            WorkItem workItem = null;
            try
            {
                workItem = workItemStore.GetWorkItem(workItemId);
            }
            catch (Exception exception)
            {
                //Handle exceptions quietly since work item may have been deleted
                LogTraceEvent(eventLog, String.Format("Unable to get work item {0} - error: ", workItemId, exception.Message), EventLogEntryType.Information);
            }

            //Make sure we have retrieved the work item (may have been deleted)
            if (remoteRequirement == null || workItem == null)
            {
                LogTraceEvent(eventLog, "Unable to retrieve both the requirement and work item, so aborting update", EventLogEntryType.Warning);
                return;
            }

            //Update the requirement with the text fields
            if (!String.IsNullOrEmpty(workItem.Title))
            {
                remoteRequirement.Name = workItem.Title;
            }
            //See if we're using a rich text or plain text description field
            if (workItem.Type.FieldDefinitions.Contains(TFS_FIELD_DESCRIPTION_RICH_TEXT) && !String.IsNullOrEmpty(workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT].ToString()))
            {
                remoteRequirement.Description = (string)workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT];
            }
            else if (!String.IsNullOrEmpty(workItem.Description))
            {
                remoteRequirement.Description = workItem.Description;
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the requirement name and description\n", EventLogEntryType.Information);

            //Now get the requirement status from the State mapping
            RemoteDataMapping dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.State, statusMappings, true);
            if (dataMapping == null)
            {
                //We can't find the matching item so log and ignore
                eventLog.WriteEntry("Unable to locate mapping entry for Requirement State " + workItem.State + " in project " + projectId, EventLogEntryType.Error);
            }
            else
            {
                remoteRequirement.StatusId = dataMapping.InternalId;
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the requirement status\n", EventLogEntryType.Information);

            //Importance
            //Now get the work item priority from the mapping (if priority is set)
            if (workItem.Fields.Contains("Priority") && workItem.Fields["Priority"].IsValid)
            {
                if (String.IsNullOrEmpty(workItem["Priority"].ToString()))
                {
                    remoteRequirement.ImportanceId = null;
                }
                else
                {
                    dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem["Priority"].ToString(), priorityMappings, true);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so log and just don't set the priority
                        eventLog.WriteEntry("Unable to locate mapping entry for work item priority " + workItem["Priority"].ToString() + " in project " + projectId, EventLogEntryType.Warning);
                    }
                    else
                    {
                        remoteRequirement.ImportanceId = dataMapping.InternalId;
                    }
                }
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the requirement importance\n", EventLogEntryType.Information);

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the requirement dates\n", EventLogEntryType.Information);

            //Update the estimated effort
            if (workItem.Fields.Contains(TFS_FIELD_COMPLETED_WORK) && workItem[TFS_FIELD_COMPLETED_WORK] != null)
            {
                double completedWorkHours = (double)workItem[TFS_FIELD_COMPLETED_WORK];
                int actualEffortMins = (int)(completedWorkHours * (double)60);
                remoteRequirement.PlannedEffort = actualEffortMins;
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the requirement work\n", EventLogEntryType.Information);

            //Now we need to see if any of the SpiraTest custom properties that map to TFS fields have changed in TFS
            ProcessWorkItemCustomFieldChanges(projectId, workItem, remoteRequirement, requirementCustomProperties, customPropertyMappingList, customPropertyValueMappingList, userMappings, spiraImportExport);

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the requirement custom properties\n", EventLogEntryType.Information);

            //Set the requirement author
            if (!String.IsNullOrEmpty((string)workItem[CoreField.CreatedBy]))
            {
                dataMapping = FindUserMappingByExternalKey((string)workItem[CoreField.CreatedBy], userMappings, spiraImportExport);
                if (dataMapping == null)
                {
                    //We can't find the matching user so log and ignore
                    eventLog.WriteEntry("Unable to locate mapping entry for TFS user " + (string)workItem[CoreField.CreatedBy] + " so using the synchronization user", EventLogEntryType.Warning);
                }
                else
                {
                    remoteRequirement.AuthorId = dataMapping.InternalId;
                    LogTraceEvent(eventLog, "Got the author " + remoteRequirement.AuthorId.ToString() + "\n", EventLogEntryType.Information);
                }
            }

            //Set the owner/assignee
            if (String.IsNullOrEmpty((string)workItem[CoreField.AssignedTo]))
            {
                remoteRequirement.OwnerId = null;
            }
            else
            {
                dataMapping = FindUserMappingByExternalKey((string)workItem[CoreField.AssignedTo], userMappings, spiraImportExport);
                if (dataMapping == null)
                {
                    //We can't find the matching user so log and ignore
                    eventLog.WriteEntry("Unable to locate mapping entry for TFS user " + (string)workItem[CoreField.AssignedTo] + " so ignoring the assignee change", EventLogEntryType.Error);
                }
                else
                {
                    remoteRequirement.OwnerId = dataMapping.InternalId;
                    LogTraceEvent(eventLog, "Got the assignee " + remoteRequirement.OwnerId.ToString() + "\n", EventLogEntryType.Information);
                }
            }

            //Specify the requirement release if applicable
            if (!String.IsNullOrEmpty(workItem.IterationPath))
            {
                //See if we have a mapped SpiraTest release
                dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), releaseMappings, false);
                if (dataMapping == null)
                {
                    //Now check to see if recently added
                    dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), newReleaseMappings.ToArray(), false);
                }
                if (dataMapping == null)
                {
                    //We can't find the matching item so need to create a new release in SpiraTest and add to mappings

                    //Need to iterate through the TFS iteration node tree to get the full node object
                    Node iterationNode = GetMatchingNode(tfsProject.IterationRootNodes, workItem.IterationId);
                    if (iterationNode != null)
                    {
                        LogTraceEvent(eventLog, "Adding new release in " + productName + " for iteration " + iterationNode.Name + "\n", EventLogEntryType.Information);
                        SpiraImportExport.RemoteRelease remoteRelease = new SpiraImportExport.RemoteRelease();
                        remoteRelease.Name = iterationNode.Name;
                        remoteRelease.VersionNumber = "TFS-" + iterationNode.Id;
                        remoteRelease.CreatorId = 1;    //System Administrator
                        remoteRelease.Active = true;
                        remoteRelease.StartDate = DateTime.Now.Date;
                        remoteRelease.EndDate = DateTime.Now.Date.AddDays(5);
                        remoteRelease.CreationDate = DateTime.Now;
                        remoteRelease.ResourceCount = 1;
                        remoteRelease.DaysNonWorking = 0;
                        remoteRelease = spiraImportExport.Release_Create(remoteRelease, null);

                        //Add a new mapping entry
                        SpiraImportExport.RemoteDataMapping newReleaseMapping = new SpiraImportExport.RemoteDataMapping();
                        newReleaseMapping.ProjectId = projectId;
                        newReleaseMapping.InternalId = remoteRelease.ReleaseId.Value;
                        newReleaseMapping.ExternalKey = iterationNode.Id.ToString();
                        newReleaseMappings.Add(newReleaseMapping);
                        remoteRequirement.ReleaseId = newReleaseMapping.InternalId;
                    }
                }
                else
                {
                    remoteRequirement.ReleaseId = dataMapping.InternalId;
                }
            }

            //Finally update the requirement in SpiraTest, exceptions get logged
            spiraImportExport.Requirement_Update(remoteRequirement);

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Successfully updated requirement in " + productName + "\n", EventLogEntryType.Information);

            //Now we need to get all the comments attached to the work item in TFS
            RevisionCollection revisions = workItem.Revisions;

            //Now get the list of comments attached to the SpiraTest requirement
            RemoteComment[] remoteComments = spiraImportExport.Requirement_RetrieveComments(requirementId);

            //Iterate through all the comments and add any to SpiraTest
            if (revisions != null)
            {
                foreach (Revision revision in revisions)
                {
                    //Add the author, date and body to the resolution
                    if (revision.Fields[CoreField.History].Value != null && revision.Fields[CoreField.History].Value.ToString() != "")
                    {
                        //See if we already have this resolution inside SpiraTest
                        bool alreadyAdded = false;
                        foreach (RemoteComment incidentComment in remoteComments)
                        {
                            if (incidentComment.Text.Trim() == ((string)revision.Fields[CoreField.History].Value).Trim())
                            {
                                alreadyAdded = true;
                            }
                        }
                        if (!alreadyAdded)
                        {
                            //Get the resolution author mapping
                            string revisionCreatedBy = (string)revision.Fields[CoreField.ChangedBy].Value;
                            LogTraceEvent(eventLog, "Looking for comments author: '" + revisionCreatedBy + "'\n", EventLogEntryType.Information);
                            int? creatorId = null;
                            dataMapping = FindUserMappingByExternalKey(revisionCreatedBy, userMappings, spiraImportExport);
                            if (dataMapping != null)
                            {
                                creatorId = dataMapping.InternalId;
                                LogTraceEvent(eventLog, "Got the resolution creator: " + creatorId.ToString() + "\n", EventLogEntryType.Information);
                            }

                            //Add the comment to SpiraTest
                            RemoteComment newComment = new RemoteComment();
                            newComment.ArtifactId = requirementId;
                            newComment.UserId = creatorId;
                            newComment.CreationDate = ((DateTime)revision.Fields[CoreField.ChangedDate].Value).ToUniversalTime();
                            newComment.Text = (string)revision.Fields[CoreField.History].Value;

                            spiraImportExport.Requirement_CreateComment(newComment);
                        }
                    }
                }
            }
            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the comments/history\n", EventLogEntryType.Information);
        }

        /// <summary>
        /// Processes a task that has been updated in TFS
        /// </summary>
        private void ProcessUpdatedTask(int projectId, ImportExportClient spiraImportExport, RemoteDataMapping taskMapping, List<RemoteDataMapping> newReleaseMappings, List<RemoteDataMapping> oldReleaseMappings, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteCustomProperty[] taskCustomProperties, Project tfsProject, WorkItemStore workItemStore, string productName, RemoteDataMapping[] priorityMappings, RemoteDataMapping[] statusMappings, RemoteDataMapping[] userMappings, RemoteDataMapping[] releaseMappings)
        {
            //Make sure it's for the current project
            if (taskMapping.ProjectId != projectId)
            {
                LogTraceEvent(eventLog, "Projects didn't match, so aborting update", EventLogEntryType.Warning);
                return;
            }

            //First we need to retrieve the task from Spira
            int taskId = taskMapping.InternalId;
            int workItemId;
            if (!Int32.TryParse(taskMapping.ExternalKey, out workItemId))
            {
                LogErrorEvent(String.Format("Unable to convert task external key '{0}' to integer, so ignoring.", taskMapping.ExternalKey), EventLogEntryType.Warning);
                return;
            }
            RemoteTask remoteTask = spiraImportExport.Task_RetrieveById(taskId);

            //Now retrieve the work item from MSTFS
            WorkItem workItem = null;
            try
            {
                workItem = workItemStore.GetWorkItem(workItemId);
            }
            catch (Exception exception)
            {
                //Handle exceptions quietly since work item may have been deleted
                LogTraceEvent(eventLog, String.Format("Unable to get work item {0} - error: ", workItemId, exception.Message), EventLogEntryType.Information);
            }

            //Make sure we have retrieved the work item (may have been deleted)
            if (remoteTask == null || workItem == null)
            {
                LogTraceEvent(eventLog, "Unable to retrieve both the task and work item, so aborting update", EventLogEntryType.Warning);
                return;
            }

            //Update the task with the text fields
            if (!String.IsNullOrEmpty(workItem.Title))
            {
                remoteTask.Name = workItem.Title;
            }
            //See if we're using a rich text or plain text description field
            if (workItem.Type.FieldDefinitions.Contains(TFS_FIELD_DESCRIPTION_RICH_TEXT) && !String.IsNullOrEmpty(workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT].ToString()))
            {
                remoteTask.Description = (string)workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT];
            }
            else if (!String.IsNullOrEmpty(workItem.Description))
            {
                remoteTask.Description = workItem.Description;
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the task name and description\n", EventLogEntryType.Information);

            //Now get the task status from the State mapping
            RemoteDataMapping dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.State, statusMappings, true);
            if (dataMapping == null)
            {
                //We can't find the matching item so log and ignore
                eventLog.WriteEntry("Unable to locate mapping entry for Task State " + workItem.State + " in project " + projectId, EventLogEntryType.Error);
            }
            else
            {
                remoteTask.TaskStatusId = dataMapping.InternalId;
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the task status\n", EventLogEntryType.Information);

            //Importance
            //Now get the work item priority from the mapping (if priority is set)
            if (workItem.Fields.Contains("Priority") && workItem.Fields["Priority"].IsValid)
            {
                if (String.IsNullOrEmpty(workItem["Priority"].ToString()))
                {
                    remoteTask.TaskPriorityId = null;
                }
                else
                {
                    dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem["Priority"].ToString(), priorityMappings, true);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so log and just don't set the priority
                        eventLog.WriteEntry("Unable to locate mapping entry for work item priority " + workItem["Priority"].ToString() + " in project " + projectId, EventLogEntryType.Warning);
                    }
                    else
                    {
                        remoteTask.TaskPriorityId = dataMapping.InternalId;
                    }
                }
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the task importance\n", EventLogEntryType.Information);

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the task dates\n", EventLogEntryType.Information);

            //Update the dates and efforts
            if (workItem[TFS_FIELD_START_DATE] != null)
            {
                remoteTask.StartDate = ((DateTime)workItem[TFS_FIELD_START_DATE]).ToUniversalTime();
            }
            if (workItem[TFS_FIELD_FINISH_DATE] != null)
            {
                remoteTask.EndDate = ((DateTime)workItem[TFS_FIELD_FINISH_DATE]).ToUniversalTime();
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the task dates\n", EventLogEntryType.Information);

            //Update the actual and estimated work
            if (workItem.Fields.Contains(TFS_FIELD_COMPLETED_WORK) && workItem[TFS_FIELD_COMPLETED_WORK] != null)
            {
                double completedWorkHours = (double)workItem[TFS_FIELD_COMPLETED_WORK];
                int actualEffortMins = (int)(completedWorkHours * (double)60);
                if (!remoteTask.EstimatedEffort.HasValue)
                {
                    remoteTask.EstimatedEffort = actualEffortMins;
                }
                remoteTask.ActualEffort = actualEffortMins;
            }

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the task work\n", EventLogEntryType.Information);

            //Now we need to see if any of the SpiraTest custom properties that map to TFS fields have changed in TFS
            ProcessWorkItemCustomFieldChanges(projectId, workItem, remoteTask, taskCustomProperties, customPropertyMappingList, customPropertyValueMappingList, userMappings, spiraImportExport);

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the task custom properties\n", EventLogEntryType.Information);

            //Set the task author
            if (!String.IsNullOrEmpty((string)workItem[CoreField.CreatedBy]))
            {
                dataMapping = FindUserMappingByExternalKey((string)workItem[CoreField.CreatedBy], userMappings, spiraImportExport);
                if (dataMapping == null)
                {
                    //We can't find the matching user so log and ignore
                    eventLog.WriteEntry("Unable to locate mapping entry for TFS user " + (string)workItem[CoreField.CreatedBy] + " so using the synchronization user", EventLogEntryType.Warning);
                }
                else
                {
                    remoteTask.CreatorId = dataMapping.InternalId;
                    LogTraceEvent(eventLog, "Got the creator " + remoteTask.CreatorId.ToString() + "\n", EventLogEntryType.Information);
                }
            }

            //Set the owner/assignee
            if (String.IsNullOrEmpty((string)workItem[CoreField.AssignedTo]))
            {
                remoteTask.OwnerId = null;
            }
            else
            {
                dataMapping = FindUserMappingByExternalKey((string)workItem[CoreField.AssignedTo], userMappings, spiraImportExport);
                if (dataMapping == null)
                {
                    //We can't find the matching user so log and ignore
                    eventLog.WriteEntry("Unable to locate mapping entry for TFS user " + (string)workItem[CoreField.AssignedTo] + " so ignoring the assignee change", EventLogEntryType.Error);
                }
                else
                {
                    remoteTask.OwnerId = dataMapping.InternalId;
                    LogTraceEvent(eventLog, "Got the assignee " + remoteTask.OwnerId.ToString() + "\n", EventLogEntryType.Information);
                }
            }

            //Specify the task release if applicable
            if (!String.IsNullOrEmpty(workItem.IterationPath))
            {
                //See if we have a mapped SpiraTest release
                dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), releaseMappings, false);
                if (dataMapping == null)
                {
                    //Now check to see if recently added
                    dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), newReleaseMappings.ToArray(), false);
                }
                if (dataMapping == null)
                {
                    //We can't find the matching item so need to create a new release in SpiraTest and add to mappings

                    //Need to iterate through the TFS iteration node tree to get the full node object
                    Node iterationNode = GetMatchingNode(tfsProject.IterationRootNodes, workItem.IterationId);
                    if (iterationNode != null)
                    {
                        LogTraceEvent(eventLog, "Adding new release in " + productName + " for iteration " + iterationNode.Name + "\n", EventLogEntryType.Information);
                        SpiraImportExport.RemoteRelease remoteRelease = new SpiraImportExport.RemoteRelease();
                        remoteRelease.Name = iterationNode.Name;
                        remoteRelease.VersionNumber = "TFS-" + iterationNode.Id;
                        remoteRelease.CreatorId = 1;    //System Administrator
                        remoteRelease.Active = true;
                        remoteRelease.StartDate = DateTime.Now.Date;
                        remoteRelease.EndDate = DateTime.Now.Date.AddDays(5);
                        remoteRelease.CreationDate = DateTime.Now;
                        remoteRelease.ResourceCount = 1;
                        remoteRelease.DaysNonWorking = 0;
                        remoteRelease = spiraImportExport.Release_Create(remoteRelease, null);

                        //Add a new mapping entry
                        SpiraImportExport.RemoteDataMapping newReleaseMapping = new SpiraImportExport.RemoteDataMapping();
                        newReleaseMapping.ProjectId = projectId;
                        newReleaseMapping.InternalId = remoteRelease.ReleaseId.Value;
                        newReleaseMapping.ExternalKey = iterationNode.Id.ToString();
                        newReleaseMappings.Add(newReleaseMapping);
                        remoteTask.ReleaseId = newReleaseMapping.InternalId;
                    }
                }
                else
                {
                    remoteTask.ReleaseId = dataMapping.InternalId;
                }
            }

            //Finally update the task in SpiraTest, exceptions get logged
            spiraImportExport.Task_Update(remoteTask);

            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Successfully updated task in " + productName + "\n", EventLogEntryType.Information);

            //Now we need to get all the comments attached to the work item in TFS
            RevisionCollection revisions = workItem.Revisions;

            //Now get the list of comments attached to the SpiraTest task
            RemoteComment[] remoteComments = spiraImportExport.Task_RetrieveComments(taskId);

            //Iterate through all the comments and add any to SpiraTest
            if (revisions != null)
            {
                foreach (Revision revision in revisions)
                {
                    //Add the author, date and body to the resolution
                    if (revision.Fields[CoreField.History].Value != null && revision.Fields[CoreField.History].Value.ToString() != "")
                    {
                        //See if we already have this resolution inside SpiraTest
                        bool alreadyAdded = false;
                        foreach (RemoteComment incidentComment in remoteComments)
                        {
                            if (incidentComment.Text.Trim() == ((string)revision.Fields[CoreField.History].Value).Trim())
                            {
                                alreadyAdded = true;
                            }
                        }
                        if (!alreadyAdded)
                        {
                            //Get the resolution author mapping
                            string revisionCreatedBy = (string)revision.Fields[CoreField.ChangedBy].Value;
                            LogTraceEvent(eventLog, "Looking for comments author: '" + revisionCreatedBy + "'\n", EventLogEntryType.Information);
                            int? creatorId = null;
                            dataMapping = FindUserMappingByExternalKey(revisionCreatedBy, userMappings, spiraImportExport);
                            if (dataMapping != null)
                            {
                                creatorId = dataMapping.InternalId;
                                LogTraceEvent(eventLog, "Got the resolution creator: " + creatorId.ToString() + "\n", EventLogEntryType.Information);
                            }

                            //Add the comment to SpiraTest
                            RemoteComment newComment = new RemoteComment();
                            newComment.ArtifactId = taskId;
                            newComment.UserId = creatorId;
                            newComment.CreationDate = ((DateTime)revision.Fields[CoreField.ChangedDate].Value).ToUniversalTime();
                            newComment.Text = (string)revision.Fields[CoreField.History].Value;

                            spiraImportExport.Task_CreateComment(newComment);
                        }
                    }
                }
            }
            //Debug logging - comment out for production code
            LogTraceEvent(eventLog, "Got the comments/history\n", EventLogEntryType.Information);
        }

        /// <summary>
        /// Processes an incident that has been updated in either Spira or TFS
        /// </summary>
        private void ProcessUpdatedIncident(int projectId, ImportExportClient spiraImportExport, RemoteDataMapping incidentMapping, List<RemoteDataMapping> newReleaseMappings, List<RemoteDataMapping> oldReleaseMappings, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteCustomProperty[] incidentCustomProperties, TfsTeamProjectCollection tfsTeamProjectCollection, Project tfsProject, WorkItemStore workItemStore, string productName, RemoteDataMapping[] severityMappings, RemoteDataMapping[] priorityMappings, RemoteDataMapping[] statusMappings, RemoteDataMapping[] typeMappings, RemoteDataMapping[] userMappings, RemoteDataMapping[] releaseMappings)
        {
            //Make sure it's for the current project
            if (incidentMapping.ProjectId != projectId)
            {
                LogTraceEvent(eventLog, "Projects didn't match, so aborting update", EventLogEntryType.Warning);
                return;
            }

            //First we need to retrieve the incident from Spira
            int incidentId = incidentMapping.InternalId;
            int workItemId;
            if (!Int32.TryParse(incidentMapping.ExternalKey, out workItemId))
            {
                LogErrorEvent(String.Format("Unable to convert incident external key '{0}' to integer, so ignoring.", incidentMapping.ExternalKey), EventLogEntryType.Warning);
                return;
            }
            RemoteIncident remoteIncident = spiraImportExport.Incident_RetrieveById(incidentId);

            //Now retrieve the work item from MSTFS
            WorkItem workItem = null;
            try
            {
                workItem = workItemStore.GetWorkItem(workItemId);
            }
            catch (Exception exception)
            {
                //Handle exceptions quietly since work item may have been deleted
                LogTraceEvent(eventLog, String.Format("Unable to get work item {0} - error: ", workItemId, exception.Message), EventLogEntryType.Information);
            }

            //Make sure we have retrieved the work item (may have been deleted)
            if (remoteIncident == null || workItem == null)
            {
                LogTraceEvent(eventLog, "Unable to retrieve both the incident and work item, so aborting update", EventLogEntryType.Warning);
                return;
            }

            //Now check to see if we have a change in TFS or SpiraTeam since we last ran
            //Only apply the timeoffset to TFS as the data-sync runs on the same server as SpiraTeam
            //Also SpiraTeam dates are in UTC whereas TFS will be in local-time
            string updateMode = "";
            DateTime spiraLastUpdateDate = remoteIncident.LastUpdateDate;
            DateTime tfsLastUpdateDate = workItem.ChangedDate.ToUniversalTime().AddHours(timeOffsetHours);
            if (tfsLastUpdateDate > spiraLastUpdateDate)
            {
                updateMode = "TFS=Newer";
            }
            else
            {
                updateMode = "Spira=Newer";
            }
            LogTraceEvent(eventLog, "Update Mode is " + updateMode + "\n", EventLogEntryType.SuccessAudit);

            //Handle the case where we need to move data SpiraTeam > TFS
            if (updateMode == "Spira=Newer")
            {
                //We need to track if any changes were made and only update in that case
                //to avoid the issue of perpetual updates between Spira and TFS
                bool changesMade = false;

                //Get certain incident fields into local variables (if used more than once)
                int incidentStatusId = remoteIncident.IncidentStatusId.Value;

                //Now get the work item type from the mapping
                //Tasks are handled separately unless they are mapped, need to check
                RemoteDataMapping dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.IncidentTypeId.Value, typeMappings);
                if (dataMapping == null)
                {
                    //We can't find the matching item so log and move to the next incident
                    eventLog.WriteEntry("Unable to locate mapping entry for incident type " + remoteIncident.IncidentTypeId + " in project " + projectId, EventLogEntryType.Error);
                    return;
                }
                string workItemTypeName = dataMapping.ExternalKey;

                //First we need to get the Iteration, mapped from the SpiraTest Release, if not create it
                //Need to do this before creating the work item as we may need to reload the project reference
                int iterationId = -1;
                if (remoteIncident.ResolvedReleaseId.HasValue)
                {
                    int detectedReleaseId = remoteIncident.ResolvedReleaseId.Value;
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, detectedReleaseId, releaseMappings);
                    if (dataMapping == null)
                    {
                        //Now check to see if recently added
                        dataMapping = InternalFunctions.FindMappingByInternalId(projectId, detectedReleaseId, newReleaseMappings.ToArray());
                    }
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so need to create a new iteration in TFS and add to mappings
                        LogTraceEvent(eventLog, "Adding new iteration in TFS for release " + detectedReleaseId + "\n", EventLogEntryType.Information);
                        Node newIterationNode = AddNewTfsIteration(tfsTeamProjectCollection, ref workItemStore, ref tfsProject, remoteIncident.ResolvedReleaseVersionNumber);

                        //Add a new mapping entry if successful
                        if (newIterationNode != null)
                        {
                            SpiraImportExport.RemoteDataMapping newReleaseMapping = new SpiraImportExport.RemoteDataMapping();
                            newReleaseMapping.ProjectId = projectId;
                            newReleaseMapping.InternalId = detectedReleaseId;
                            newReleaseMapping.ExternalKey = newIterationNode.Id.ToString();
                            newReleaseMappings.Add(newReleaseMapping);
                            iterationId = newIterationNode.Id;
                        }
                    }
                    else
                    {
                        if (!Int32.TryParse(dataMapping.ExternalKey, out iterationId))
                        {
                            iterationId = -1;
                            eventLog.WriteEntry("The release/iteration external key " + dataMapping.ExternalKey + " in project " + projectId + " is invalid - it needs to be numeric!", EventLogEntryType.Warning);
                        }
                    }
                }
                //If we don't have a Resolved Release set, take the value from the Detected Release instead
                if (iterationId == -1 && remoteIncident.DetectedReleaseId.HasValue)
                {
                    int detectedReleaseId = remoteIncident.DetectedReleaseId.Value;
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, detectedReleaseId, releaseMappings);
                    if (dataMapping == null)
                    {
                        //Now check to see if recently added
                        dataMapping = InternalFunctions.FindMappingByInternalId(projectId, detectedReleaseId, newReleaseMappings.ToArray());
                    }
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so need to create a new iteration in TFS and add to mappings
                        LogTraceEvent(eventLog, "Adding new iteration in TFS for release " + detectedReleaseId + "\n", EventLogEntryType.Information);
                        Node newIterationNode = AddNewTfsIteration(tfsTeamProjectCollection, ref workItemStore, ref tfsProject, remoteIncident.DetectedReleaseVersionNumber);

                        //Add a new mapping entry if successful
                        if (newIterationNode != null)
                        {
                            SpiraImportExport.RemoteDataMapping newReleaseMapping = new SpiraImportExport.RemoteDataMapping();
                            newReleaseMapping.ProjectId = projectId;
                            newReleaseMapping.InternalId = detectedReleaseId;
                            newReleaseMapping.ExternalKey = newIterationNode.Id.ToString();
                            newReleaseMappings.Add(newReleaseMapping);
                            iterationId = newIterationNode.Id;
                        }
                    }
                    else
                    {
                        if (!Int32.TryParse(dataMapping.ExternalKey, out iterationId))
                        {
                            iterationId = -1;
                            eventLog.WriteEntry("The release/iteration external key " + dataMapping.ExternalKey + " in project " + projectId + " is invalid - it needs to be numeric!", EventLogEntryType.Warning);
                        }
                    }
                }

                //Now, update the new TFS work item, populating first the standard fields that don't need mapping
                if (workItem.Title != remoteIncident.Name)
                {
                    workItem.Title = remoteIncident.Name;
                    changesMade = true;
                }
                //The description field only supports plain text
                //See if we're using the alternative "Steps to Reproduce" rich text
                if (workItem.Type.FieldDefinitions.Contains(TFS_FIELD_STEPS_TO_REPRODUCE) && !String.IsNullOrEmpty(workItem[TFS_FIELD_STEPS_TO_REPRODUCE].ToString()))
                {
                    string stepsToReproduce = (string)workItem[TFS_FIELD_STEPS_TO_REPRODUCE];
                    if (stepsToReproduce != remoteIncident.Description)
                    {
                        workItem[TFS_FIELD_STEPS_TO_REPRODUCE] = remoteIncident.Description;
                        changesMade = true;
                    }
                }
                else if (workItem.Type.FieldDefinitions.Contains(TFS_FIELD_DESCRIPTION_RICH_TEXT) && !String.IsNullOrEmpty(workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT].ToString()))
                {
                    string richTextDesc = (string)workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT];
                    if (richTextDesc != remoteIncident.Description)
                    {
                        workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT] = remoteIncident.Description;
                        changesMade = true;
                    }
                }
                else
                {
                    string description = InternalFunctions.HtmlRenderAsPlainText(remoteIncident.Description);
                    if (workItem.Description != description)
                    {
                        workItem.Description = description;
                        changesMade = true;
                    }
                }
                if (iterationId != -1 && workItem.IterationId != iterationId)
                {
                    changesMade = true;
                    workItem.IterationId = iterationId;
                }

                //Update the special detector field name custom property if appropriate
                if (!String.IsNullOrEmpty(this.incidentDetectorTfsField) && workItem.Type.FieldDefinitions.Contains(this.incidentDetectorTfsField))
                {
                    if (workItem[this.incidentDetectorTfsField].ToString() != remoteIncident.OpenerName)
                    {
                        workItem[this.incidentDetectorTfsField] = remoteIncident.OpenerName;
                        changesMade = true;
                    }
                }

                //Now get the incident status from the mapping
                dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.IncidentStatusId.Value, statusMappings);
                if (dataMapping == null)
                {
                    //We can't find the matching item so log and move to the next incident
                    eventLog.WriteEntry("Unable to locate mapping entry for incident status " + remoteIncident.IncidentStatusId + " in project " + projectId, EventLogEntryType.Error);
                    return;
                }
                //The status in SpiraTest = MSTFS State+Reason
                string[] stateAndReason = dataMapping.ExternalKey.Split('+');
                string tfsState = stateAndReason[0];
                string tfsReason = stateAndReason[1];

                //Now get the incident priority from the mapping (if priority is set)
                if (remoteIncident.PriorityId.HasValue)
                {
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.PriorityId.Value, priorityMappings);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so log and just don't set the priority
                        eventLog.WriteEntry("Unable to locate mapping entry for incident priority " + remoteIncident.PriorityId.Value + " in project " + projectId, EventLogEntryType.Warning);
                    }
                    else
                    {
                        if (workItem.Type.FieldDefinitions.Contains(TFS_FIELD_PRIORITY) && workItem.Fields[TFS_FIELD_PRIORITY].IsEditable)
                        {
                            if (workItem[TFS_FIELD_PRIORITY].ToString() != dataMapping.ExternalKey)
                            {
                                workItem[TFS_FIELD_PRIORITY] = dataMapping.ExternalKey;
                                changesMade = true;
                            }
                        }
                    }
                }

                //Now get the incident severity from the mapping (if severity is set)
                if (remoteIncident.SeverityId.HasValue)
                {
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.SeverityId.Value, severityMappings);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so log and just don't set the severity
                        eventLog.WriteEntry("Unable to locate mapping entry for incident severity " + remoteIncident.SeverityId.Value + " in project " + projectId, EventLogEntryType.Warning);
                    }
                    else
                    {
                        if (workItem.Type.FieldDefinitions.Contains(TFS_FIELD_SEVERITY) && workItem.Fields[TFS_FIELD_SEVERITY].IsEditable)
                        {
                            if (workItem[TFS_FIELD_SEVERITY].ToString() != dataMapping.ExternalKey)
                            {
                                workItem[TFS_FIELD_SEVERITY] = dataMapping.ExternalKey;
                                changesMade = true;
                            }
                        }
                    }
                }

                //See if the creator is allowed to be set on the work-item
                if (workItem.Type.FieldDefinitions[CoreField.CreatedBy].IsEditable)
                {
                    dataMapping = FindUserMappingByInternalId(remoteIncident.OpenerId.Value, userMappings, spiraImportExport);
                    if (dataMapping == null)
                    {
                        //We can't find the matching user so ignore
                        eventLog.WriteEntry("Unable to locate mapping entry for user id " + remoteIncident.OpenerId + " so leaving blank", EventLogEntryType.Warning);
                    }
                    else
                    {
                        if (workItem[CoreField.CreatedBy].ToString() != dataMapping.ExternalKey)
                        {
                            workItem[CoreField.CreatedBy] = dataMapping.ExternalKey;
                            changesMade = true;
                        }
                    }
                }

                //Now set the assignee
                if (remoteIncident.OwnerId.HasValue)
                {
                    dataMapping = FindUserMappingByInternalId(remoteIncident.OwnerId.Value, userMappings, spiraImportExport);
                    if (dataMapping == null)
                    {
                        //We can't find the matching user so ignore
                        eventLog.WriteEntry("Unable to locate mapping entry for user id " + remoteIncident.OwnerId.Value + " so leaving blank", EventLogEntryType.Warning);
                    }
                    else
                    {
                        if (workItem[CoreField.AssignedTo].ToString() != dataMapping.ExternalKey)
                        {
                            workItem[CoreField.AssignedTo] = dataMapping.ExternalKey;
                            changesMade = true;
                        }
                    }
                }

                //Now iterate through the incident custom properties
                changesMade = ProcessIncidentCustomProperties(productName, projectId, remoteIncident, workItem, customPropertyMappingList, customPropertyValueMappingList, userMappings, spiraImportExport, changesMade);

                //Set the state and reason
                if (workItem.State != tfsState)
                {
                    workItem.State = tfsState;
                    changesMade = true;
                }
                if (workItem.Reason != tfsReason)
                {
                    workItem.Reason = tfsReason;
                    changesMade = true;
                }

                //Finally add any new comments to the work item
                RemoteComment[] remoteComments = spiraImportExport.Incident_RetrieveComments(incidentId);
                LogTraceEvent(eventLog, "Checking to see if we need to add " + productName + " resolutions to TFS for incident IN" + remoteIncident.IncidentId + ". There are currently " + remoteComments.Length + " comments in " + productName + " and " + workItem.Revisions.Count + " revisions in TFS", EventLogEntryType.Information);
                foreach (RemoteComment remoteComment in remoteComments)
                {
                    //LogTraceEvent(eventLog, "Found existing " + productName + " resolution '" + remoteResolution.Resolution + "'", EventLogEntryType.Information);
                    //See if we have any existing TFS comments
                    bool anyCommentsAlreadyInTfs = false;
                    bool matchFound = false;
                    foreach (Revision revision in workItem.Revisions)
                    {
                        //Add the author, date and body to the resolution
                        if (revision.Fields[CoreField.History].Value != null && revision.Fields[CoreField.History].Value.ToString() != "")
                        {
                            //LogTraceEvent(eventLog, "Found existing TFS comment '" + revision.Fields[CoreField.History].Value + "'", EventLogEntryType.Information);
                            anyCommentsAlreadyInTfs = true;
                            //See if we have one that's not already there
                            string resolutionDescription = remoteComment.Text;
                            if (((string)revision.Fields[CoreField.History].Value).Trim() == resolutionDescription.Trim())
                            {
                                matchFound = true;
                                break;
                            }
                        }
                    }

                    //If there are no comments at all in TFS, then we should add
                    if (!anyCommentsAlreadyInTfs || !matchFound)
                    {
                        LogTraceEvent(eventLog, "Adding new comment '" + remoteComment.Text + "' to TFS (no existing comments)", EventLogEntryType.Information);
                        workItem.History = remoteComment.Text;
                        changesMade = true;
                    }
                }

                if (changesMade)
                {
                    //Validate the work item
                    StringBuilder messages = new StringBuilder();
                    if (ValidateItem(workItem, messages))
                    {
                        //Finally save the new work item
                        try
                        {
                            workItem.Save();
                        }
                        catch (Exception exception)
                        {
                            //If we have TFS error TF26201
                            //Iterate through the fields to see which ones caused the error
                            if (exception.Message.Contains("TF26201"))
                            {
                                bool foundInvalidField = false;
                                foreach (Field field in workItem.Fields)
                                {
                                    if (!field.IsValid)
                                    {
                                        //Log the name of the field
                                        foundInvalidField = true;
                                        eventLog.WriteEntry("Error Updating " + productName + " Incident in Team Foundation Server because of bad field '" + field.Name + "' (" + exception.Message + ")", EventLogEntryType.Error);
                                    }
                                }
                                if (!foundInvalidField)
                                {
                                    //Log a general exception
                                    eventLog.WriteEntry("Error Updating " + productName + " Incident in Team Foundation Server: " + exception.Message, EventLogEntryType.Error);
                                }
                            }
                            else
                            {
                                throw exception;
                            }
                        }
                    }
                    else
                    {
                        //Log the detailed error message
                        eventLog.WriteEntry("Error Updating " + productName + " Incident in Team Foundation Server: " + messages.ToString(), EventLogEntryType.Error);
                    }
                }
            }

            //Handle the case where we need to move data TFS > SpiraTeam
            if (updateMode == "TFS=Newer")
            {
                RemoteDataMapping dataMapping;

                //We need to track if any changes were made and only update in that case
                //to avoid the issue of perpetual updates
                bool changesMade = false;

                //Update the incident with the text fields
                if (!String.IsNullOrEmpty(workItem.Title) && remoteIncident.Name != workItem.Title)
                {
                    remoteIncident.Name = workItem.Title;
                    changesMade = true;
                }

                //See if we're using the plain-text description field or the rich-text
                //steps to reproduce field
                if (workItem.Type.FieldDefinitions.Contains(TFS_FIELD_STEPS_TO_REPRODUCE) && !String.IsNullOrEmpty(workItem[TFS_FIELD_STEPS_TO_REPRODUCE].ToString()))
                {
                    remoteIncident.Description = (string)workItem[TFS_FIELD_STEPS_TO_REPRODUCE];
                    changesMade = true;
                }
                else if (workItem.Type.FieldDefinitions.Contains(TFS_FIELD_DESCRIPTION_RICH_TEXT) && !String.IsNullOrEmpty(workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT].ToString()))
                {
                    remoteIncident.Description = (string)workItem[TFS_FIELD_DESCRIPTION_RICH_TEXT];
                    changesMade = true;
                }
                else
                {
                    if (String.IsNullOrEmpty(workItem.Description))
                    {
                        remoteIncident.Description = "Empty Description in TFS";
                    }
                    else
                    {
                        if (remoteIncident.Description != workItem.Description)
                        {
                            remoteIncident.Description = workItem.Description;
                            changesMade = true;
                        }
                    }
                }

                //Debug logging - comment out for production code
                LogTraceEvent(eventLog, "Got the incident name and description\n", EventLogEntryType.Information);

                //Now get the work item priority from the mapping (if priority is set)
                try
                {
                    if (workItem.Fields.Contains(TFS_FIELD_PRIORITY) && workItem.Fields[TFS_FIELD_PRIORITY].IsValid)
                    {
                        if (String.IsNullOrEmpty(workItem[TFS_FIELD_PRIORITY].ToString()))
                        {
                            if (remoteIncident.PriorityId.HasValue)
                            {
                                remoteIncident.PriorityId = null;
                                changesMade = true;
                            }
                        }
                        else
                        {
                            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem[TFS_FIELD_PRIORITY].ToString(), priorityMappings, true);
                            if (dataMapping == null)
                            {
                                //We can't find the matching item so log and just don't set the priority
                                eventLog.WriteEntry("Unable to locate mapping entry for work item priority " + workItem[TFS_FIELD_PRIORITY].ToString() + " in project " + projectId, EventLogEntryType.Warning);
                            }
                            else
                            {
                                if (!remoteIncident.PriorityId.HasValue || remoteIncident.PriorityId != dataMapping.InternalId)
                                {
                                    changesMade = true;
                                    remoteIncident.PriorityId = dataMapping.InternalId;
                                }
                            }
                        }

                        //Debug logging - comment out for production code
                        LogTraceEvent(eventLog, "Got the priority\n", EventLogEntryType.Information);
                    }
                }
                catch (Exception exception)
                {
                    if (exception.Message.Contains("TF26027"))
                    {
                        //This is because we don't have the priority field defined, just ignore
                    }
                    else
                    {
                        throw exception;
                    }
                }

                //Now get the work item severity from the mapping (if severity is set)
                try
                {
                    if (workItem.Fields.Contains(TFS_FIELD_SEVERITY) && workItem.Fields[TFS_FIELD_SEVERITY].IsValid)
                    {
                        if (String.IsNullOrEmpty(workItem[TFS_FIELD_SEVERITY].ToString()))
                        {
                            if (remoteIncident.SeverityId.HasValue)
                            {
                                remoteIncident.SeverityId = null;
                                changesMade = true;
                            }
                        }
                        else
                        {
                            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem[TFS_FIELD_SEVERITY].ToString(), severityMappings, true);
                            if (dataMapping == null)
                            {
                                //We can't find the matching item so log and just don't set the severity
                                eventLog.WriteEntry("Unable to locate mapping entry for work item severity " + workItem[TFS_FIELD_SEVERITY].ToString() + " in project " + projectId, EventLogEntryType.Warning);
                            }
                            else
                            {
                                if (!remoteIncident.SeverityId.HasValue || remoteIncident.SeverityId != dataMapping.InternalId)
                                {
                                    changesMade = true;
                                    remoteIncident.SeverityId = dataMapping.InternalId;
                                }
                            }
                        }

                        //Debug logging - comment out for production code
                        LogTraceEvent(eventLog, "Got the severity\n", EventLogEntryType.Information);
                    }
                }
                catch (Exception exception)
                {
                    if (exception.Message.Contains("TF26027"))
                    {
                        //This is because we don't have the severity field defined, just ignore
                    }
                    else
                    {
                        throw exception;
                    }
                }


                //Now get the work item status from the State+Reason mapping
                string stateAndReason = workItem.State + "+" + workItem.Reason;
                dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, stateAndReason, statusMappings, true);
                if (dataMapping == null)
                {
                    //We can't find the matching item so log and ignore
                    eventLog.WriteEntry("Unable to locate mapping entry for State+Reason " + stateAndReason + " in project " + projectId, EventLogEntryType.Error);
                }
                else
                {
                    if (remoteIncident.IncidentStatusId != dataMapping.InternalId)
                    {
                        remoteIncident.IncidentStatusId = dataMapping.InternalId;
                        changesMade = true;
                    }
                }

                //Debug logging - comment out for production code
                LogTraceEvent(eventLog, "Got the incident status\n", EventLogEntryType.Information);

                //Now we need to see if any of the SpiraTest custom properties that map to TFS fields have changed in TFS
                changesMade = ProcessWorkItemCustomFieldChanges(projectId, workItem, remoteIncident, incidentCustomProperties, customPropertyMappingList, customPropertyValueMappingList, userMappings, spiraImportExport, changesMade);

                //Now we need to get all the comments attached to the work item in TFS
                RevisionCollection revisions = workItem.Revisions;

                //Now get the list of comments attached to the SpiraTest incident
                RemoteComment[] remoteComments = spiraImportExport.Incident_RetrieveComments(incidentId);

                //Iterate through all the comments and see if we need to add any to SpiraTest
                List<RemoteComment> newIncidentComments = new List<RemoteComment>();
                if (revisions != null)
                {
                    foreach (Revision revision in revisions)
                    {
                        //Add the author, date and body to the resolution
                        if (revision.Fields[CoreField.History].Value != null && revision.Fields[CoreField.History].Value.ToString() != "")
                        {
                            //See if we already have this resolution inside SpiraTest
                            bool alreadyAdded = false;
                            foreach (RemoteComment incidentComment in remoteComments)
                            {
                                if (incidentComment.Text.Trim() == ((string)revision.Fields[CoreField.History].Value).Trim())
                                {
                                    alreadyAdded = true;
                                }
                            }
                            if (!alreadyAdded)
                            {
                                //Get the resolution author mapping
                                string revisionCreatedBy = (string)revision.Fields[CoreField.ChangedBy].Value;
                                LogTraceEvent(eventLog, "Looking for comments author: '" + revisionCreatedBy + "'\n", EventLogEntryType.Information);
                                int? creatorId = null;
                                dataMapping = FindUserMappingByExternalKey(revisionCreatedBy, userMappings, spiraImportExport);
                                if (dataMapping != null)
                                {
                                    creatorId = dataMapping.InternalId;
                                    LogTraceEvent(eventLog, "Got the resolution creator: " + creatorId.ToString() + "\n", EventLogEntryType.Information);
                                }
 
                                //Add the comment to SpiraTest
                                RemoteComment newIncidentComment = new RemoteComment();
                                newIncidentComment.ArtifactId = incidentId;
                                newIncidentComment.UserId = creatorId;
                                newIncidentComment.CreationDate = ((DateTime)revision.Fields[CoreField.ChangedDate].Value).ToUniversalTime();
                                newIncidentComment.Text = (string)revision.Fields[CoreField.History].Value;
                                newIncidentComments.Add(newIncidentComment);
                                changesMade = true;
                            }
                        }
                    }
                }

                //We don't add the comments now because it will break the incident concurrency.
                //We add them after the main update

                //Debug logging - comment out for production code
                LogTraceEvent(eventLog, "Got the comments/history\n", EventLogEntryType.Information);

                if (String.IsNullOrEmpty((string)workItem[CoreField.AssignedTo]))
                {
                    if (remoteIncident.OwnerId.HasValue)
                    {
                        remoteIncident.OwnerId = null;
                        changesMade = true;
                    }
                }
                else
                {
                    dataMapping = FindUserMappingByExternalKey((string)workItem[CoreField.AssignedTo], userMappings, spiraImportExport);
                    if (dataMapping == null)
                    {
                        //We can't find the matching user so log and ignore
                        eventLog.WriteEntry("Unable to locate mapping entry for TFS user " + (string)workItem[CoreField.AssignedTo] + " so ignoring the assignee change", EventLogEntryType.Error);
                    }
                    else
                    {
                        if (!remoteIncident.OwnerId.HasValue || remoteIncident.OwnerId != dataMapping.InternalId)
                        {
                            remoteIncident.OwnerId = dataMapping.InternalId;
                            LogTraceEvent(eventLog, "Got the assignee " + remoteIncident.OwnerId.ToString() + "\n", EventLogEntryType.Information);
                            changesMade = true;
                        }
                    }
                }

                //Specify the resolved-in release if applicable
                if (!String.IsNullOrEmpty(workItem.IterationPath))
                {
                    //See if we have a mapped SpiraTest release
                    dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), releaseMappings, false);
                    if (dataMapping == null)
                    {
                        //Now check to see if recently added
                        dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), newReleaseMappings.ToArray(), false);
                    }
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so need to create a new release in SpiraTest and add to mappings

                        //Need to iterate through the TFS iteration node tree to get the full node object
                        Node iterationNode = GetMatchingNode(tfsProject.IterationRootNodes, workItem.IterationId);
                        if (iterationNode != null)
                        {
                            LogTraceEvent(eventLog, "Adding new release in " + productName + " for iteration " + iterationNode.Name + "\n", EventLogEntryType.Information);
                            SpiraImportExport.RemoteRelease remoteRelease = new SpiraImportExport.RemoteRelease();
                            remoteRelease.Name = iterationNode.Name;
                            remoteRelease.VersionNumber = "TFS-" + iterationNode.Id;
                            remoteRelease.CreatorId = remoteIncident.OpenerId;
                            remoteRelease.Active = true;
                            remoteRelease.StartDate = DateTime.Now.Date;
                            remoteRelease.EndDate = DateTime.Now.Date.AddDays(5);
                            remoteRelease.CreationDate = DateTime.Now;
                            remoteRelease.ResourceCount = 1;
                            remoteRelease.DaysNonWorking = 0;
                            remoteRelease = spiraImportExport.Release_Create(remoteRelease, null);

                            //Add a new mapping entry
                            SpiraImportExport.RemoteDataMapping newReleaseMapping = new SpiraImportExport.RemoteDataMapping();
                            newReleaseMapping.ProjectId = projectId;
                            newReleaseMapping.InternalId = remoteRelease.ReleaseId.Value;
                            newReleaseMapping.ExternalKey = iterationNode.Id.ToString();
                            newReleaseMappings.Add(newReleaseMapping);
                            remoteIncident.ResolvedReleaseId = newReleaseMapping.InternalId;
                            changesMade = true;
                        }
                    }
                    else
                    {
                        if (remoteIncident.ResolvedReleaseId != dataMapping.InternalId)
                        {
                            remoteIncident.ResolvedReleaseId = dataMapping.InternalId;
                            changesMade = true;
                        }
                    }
                }

                //Finally update the incident in SpiraTest
                if (changesMade)
                {
                    spiraImportExport.Incident_Update(remoteIncident);

                    //Debug logging - comment out for production code
                    LogTraceEvent(eventLog, "Successfully updated\n", EventLogEntryType.Information);
                }

                //Now add any comments (after the update)
                spiraImportExport.Incident_AddComments(newIncidentComments.ToArray());
            }
        }

        /// <summary>
        /// Updates the work item with any incident custom property changes
        /// </summary>
        /// <returns>True if a change was made</returns>
        private bool ProcessIncidentCustomProperties(string productName, int projectId, RemoteArtifact remoteArtifact, WorkItem workItem, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteDataMapping[] userMappings, ImportExportClient spiraImportExport, bool changesMade = false)
        {
            if (remoteArtifact.CustomProperties != null && remoteArtifact.CustomProperties.Length > 0)
            {
                foreach (RemoteArtifactCustomProperty artifactCustomProperty in remoteArtifact.CustomProperties)
                {
                    //Handle user, list and non-list separately since only the list types need to have value mappings
                    RemoteCustomProperty customProperty = artifactCustomProperty.Definition;
                    if (customProperty != null && customProperty.CustomPropertyId.HasValue)
                    {
                        if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.List)
                        {
                            //Single-Select List
                            LogTraceEvent(eventLog, "Checking list custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);

                            //See if we have a custom property value set
                            //Get the corresponding external custom field (if there is one)
                            if (artifactCustomProperty.IntegerValue.HasValue && customPropertyMappingList != null && customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                            {
                                LogTraceEvent(eventLog, "Got value for list custom property: " + customProperty.Name + " (" + artifactCustomProperty.IntegerValue.Value + ")\n", EventLogEntryType.Information);
                                SpiraImportExport.RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                                if (customPropertyDataMapping != null)
                                {
                                    string externalCustomField = customPropertyDataMapping.ExternalKey;

                                    //Get the corresponding external custom field value (if there is one)
                                    if (!String.IsNullOrEmpty(externalCustomField) && customPropertyValueMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                                    {
                                        SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId.Value];
                                        if (customPropertyValueMappings != null)
                                        {
                                            SpiraImportExport.RemoteDataMapping customPropertyValueMapping = InternalFunctions.FindMappingByInternalId(projectId, artifactCustomProperty.IntegerValue.Value, customPropertyValueMappings);
                                            if (customPropertyValueMapping != null)
                                            {
                                                string externalCustomFieldValue = customPropertyValueMapping.ExternalKey;

                                                //See if we have one of the special standard TFS fields that it maps to
                                                if (!String.IsNullOrEmpty(externalCustomFieldValue))
                                                {
                                                    if (externalCustomField == TFS_SPECIAL_FIELD_AREA)
                                                    {
                                                        LogTraceEvent(eventLog, "The custom property corresponds to the TFS Area field\n", EventLogEntryType.Information);
                                                        //Now set the value of the work item's area
                                                        int areaId = -1;
                                                        if (Int32.TryParse(externalCustomFieldValue, out areaId))
                                                        {
                                                            if (workItem.AreaId != areaId)
                                                            {
                                                                workItem.AreaId = areaId;
                                                                changesMade = true;
                                                            }
                                                        }
                                                        else
                                                        {
                                                            eventLog.WriteEntry("The area external key " + externalCustomFieldValue + " in project " + projectId + " is invalid - it needs to be numeric!", EventLogEntryType.Warning);
                                                        }
                                                    }
                                                    else
                                                    {
                                                        LogTraceEvent(eventLog, "The custom property corresponds to the TFS '" + externalCustomField + "' field", EventLogEntryType.Information);
                                                        if (workItem.Type.FieldDefinitions.Contains(externalCustomField))
                                                        {
                                                            //This is just a normal TFS custom field
                                                            if (workItem[externalCustomField].ToSafeString() != externalCustomFieldValue)
                                                            {
                                                                workItem[externalCustomField] = externalCustomFieldValue;
                                                                changesMade = true;
                                                            }
                                                        }
                                                        else
                                                        {
                                                            eventLog.WriteEntry("The custom property external key " + externalCustomField + " in project " + projectId + " does not exist in the list of TFS field definitions for this work item type!", EventLogEntryType.Warning);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        else if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.MultiList)
                        {
                            //Multi-Select List
                            LogTraceEvent(eventLog, "Checking multi-list custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);

                            //See if we have a custom property value set
                            //Get the corresponding external custom field (if there is one)
                            if (artifactCustomProperty.IntegerListValue != null && artifactCustomProperty.IntegerListValue.Length > 0 && customPropertyMappingList != null && customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                            {
                                LogTraceEvent(eventLog, "Got values for multi-list custom property: " + customProperty.Name + " (Count=" + artifactCustomProperty.IntegerListValue.Length + ")\n", EventLogEntryType.Information);
                                SpiraImportExport.RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                                if (customPropertyDataMapping != null && !String.IsNullOrEmpty(customPropertyDataMapping.ExternalKey))
                                {
                                    string externalCustomField = customPropertyDataMapping.ExternalKey;
                                    LogTraceEvent(eventLog, "Got external key for multi-list custom property: " + customProperty.Name + " = " + externalCustomField + "\n", EventLogEntryType.Information);

                                    //Loop through each value in the list
                                    List<string> externalCustomFieldValues = new List<string>();
                                    foreach (int customPropertyListValue in artifactCustomProperty.IntegerListValue)
                                    {
                                        //Get the corresponding external custom field value (if there is one)
                                        if (customPropertyValueMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                                        {
                                            SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId.Value];
                                            if (customPropertyValueMappings != null)
                                            {
                                                SpiraImportExport.RemoteDataMapping customPropertyValueMapping = InternalFunctions.FindMappingByInternalId(projectId, customPropertyListValue, customPropertyValueMappings);
                                                if (customPropertyValueMapping != null)
                                                {
                                                    LogTraceEvent(eventLog, "Added multi-list custom property field value: " + customProperty.Name + " (Value=" + customPropertyValueMapping.ExternalKey + ")\n", EventLogEntryType.Information);
                                                    externalCustomFieldValues.Add(customPropertyValueMapping.ExternalKey);
                                                }
                                            }
                                        }
                                    }

                                    //See if we have one of the special standard TFS field that it maps to
                                    LogTraceEvent(eventLog, "Got mapped values for multi-list custom property: " + customProperty.Name + " (Count=" + externalCustomFieldValues.Count + ")\n", EventLogEntryType.Information);
                                    if (externalCustomFieldValues.Count > 0)
                                    {
                                        if (externalCustomField == TFS_SPECIAL_FIELD_AREA)
                                        {
                                            //The Area field only accepts a single value, so we need to map a Spira list field to it instead
                                            LogErrorEvent("Unable to set a value on TFS Area field because the custom property is a Multi-List, please change to List property.", EventLogEntryType.Warning);
                                        }
                                        else
                                        {
                                            if (workItem.Type.FieldDefinitions.Contains(externalCustomField))
                                            {
                                                //This is just a normal TFS custom field
                                                //Multiple values in TFS are implemented using a Codeplex solution:
                                                //http://blogs.msdn.com/b/visualstudioalm/archive/2013/02/15/multivaluelist-control-in-tfs-work-item-tracking.aspx
                                                //So we need to convert the values into a semicolon separated list
                                                string valueList = "";
                                                foreach (string externalCustomFieldValue in externalCustomFieldValues)
                                                {
                                                    if (valueList == "")
                                                    {
                                                        valueList = externalCustomFieldValue;
                                                    }
                                                    else
                                                    {
                                                        valueList += ";" + externalCustomFieldValue;
                                                    }
                                                }
                                                if (workItem[externalCustomField].ToSafeString() != valueList)
                                                {
                                                    workItem[externalCustomField] = valueList;
                                                    changesMade = true;
                                                }
                                            }
                                            else
                                            {
                                                eventLog.WriteEntry("The custom property external key " + externalCustomField + " in project " + projectId + " does not exist in the list of TFS field definitions for this work item type!", EventLogEntryType.Warning);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        else if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.User)
                        {
                            //User
                            LogTraceEvent(eventLog, "Checking user custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);

                            //See if we have a custom property value set
                            if (artifactCustomProperty.IntegerValue.HasValue)
                            {
                                SpiraImportExport.RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                                if (customPropertyDataMapping != null && !String.IsNullOrEmpty(customPropertyDataMapping.ExternalKey))
                                {
                                    string externalCustomField = customPropertyDataMapping.ExternalKey;
                                    LogTraceEvent(eventLog, "Got external key for user custom property: " + customProperty.Name + " = " + externalCustomField + "\n", EventLogEntryType.Information);

                                    LogTraceEvent(eventLog, "Got value for user custom property: " + customProperty.Name + " (" + artifactCustomProperty.IntegerValue.Value + ")\n", EventLogEntryType.Information);
                                    //Get the corresponding TFS user (if there is one)
                                    RemoteDataMapping dataMapping = FindUserMappingByInternalId(artifactCustomProperty.IntegerValue.Value, userMappings, spiraImportExport);
                                    if (dataMapping != null)
                                    {
                                        string tfsUserName = dataMapping.ExternalKey;
                                        LogTraceEvent(eventLog, "Adding user custom property field value: " + customProperty.Name + " (Value=" + tfsUserName + ")\n", EventLogEntryType.Information);
                                        LogTraceEvent(eventLog, "The custom property corresponds to the TFS '" + externalCustomField + "' field", EventLogEntryType.Information);
                                        if (workItem.Type.FieldDefinitions.Contains(externalCustomField))
                                        {
                                            //This is just a normal TFS custom field
                                            if (workItem[externalCustomField].ToSafeString() != tfsUserName)
                                            {
                                                workItem[externalCustomField] = tfsUserName;
                                                changesMade = true;
                                            }
                                        }
                                        else
                                        {
                                            eventLog.WriteEntry("The custom property external key " + externalCustomField + " in project " + projectId + " does not exist in the list of TFS field definitions for this work item type!", EventLogEntryType.Warning);
                                        }
                                    }
                                    else
                                    {
                                        LogErrorEvent("Unable to find a matching TFS user for " + productName + " user with ID=" + artifactCustomProperty.IntegerValue.Value + " so leaving property null.", EventLogEntryType.Warning);
                                    }
                                }
                            }
                        }
                        else if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.Date)
                        {
                            //Date
                            LogTraceEvent(eventLog, "Checking Date custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);

                            //See if we have a custom property value set
                            if (artifactCustomProperty.DateTimeValue.HasValue)
                            {
                                LogTraceEvent(eventLog, "Got value for Date custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);
                                //Get the corresponding external custom field (if there is one)
                                if (customPropertyMappingList != null && customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                                {
                                    SpiraImportExport.RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                                    if (customPropertyDataMapping != null)
                                    {
                                        string externalCustomField = customPropertyDataMapping.ExternalKey;

                                        LogTraceEvent(eventLog, "The custom property corresponds to the TFS '" + externalCustomField + "' field", EventLogEntryType.Information);
                                        if (workItem.Type.FieldDefinitions.Contains(externalCustomField))
                                        {
                                            //We need to convert to Local Time for TFS
                                            DateTime utcDateTime = artifactCustomProperty.DateTimeValue.Value;
                                            DateTime localDateTime = utcDateTime.ToLocalTime();
                                            if (workItem[externalCustomField].ToSafeString() != localDateTime.ToSafeString())
                                            {
                                                workItem[externalCustomField] = localDateTime;
                                                changesMade = true;
                                            }
                                        }
                                        else
                                        {
                                            eventLog.WriteEntry("The custom property external key " + externalCustomField + " in project " + projectId + " does not exist in the list of TFS field definitions for this work item type!", EventLogEntryType.Warning);
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            //Other
                            LogTraceEvent(eventLog, "Checking scalar custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);

                            //See if we have a custom property value set
                            if (!String.IsNullOrEmpty(artifactCustomProperty.StringValue) || artifactCustomProperty.BooleanValue.HasValue
                                || artifactCustomProperty.DecimalValue.HasValue || artifactCustomProperty.IntegerValue.HasValue)
                            {
                                LogTraceEvent(eventLog, "Got value for scalar custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);
                                //Get the corresponding external custom field (if there is one)
                                if (customPropertyMappingList != null && customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                                {
                                    SpiraImportExport.RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                                    if (customPropertyDataMapping != null)
                                    {
                                        string externalCustomField = customPropertyDataMapping.ExternalKey;

                                        //See if we have one of the special standard TFS field that it maps to
                                        if (!String.IsNullOrEmpty(externalCustomField))
                                        {
                                            if (externalCustomField == TFS_SPECIAL_FIELD_INCIDENT_ID)
                                            {
                                                //Handled later
                                            }
                                            else
                                            {
                                                LogTraceEvent(eventLog, "The custom property corresponds to the TFS '" + externalCustomField + "' field", EventLogEntryType.Information);
                                                if (workItem.Type.FieldDefinitions.Contains(externalCustomField))
                                                {
                                                    //This is just a normal TFS custom field
                                                    object customFieldValue = InternalFunctions.GetCustomPropertyValue(artifactCustomProperty);
                                                    if (workItem[externalCustomField].ToSafeString() != customFieldValue.ToSafeString())
                                                    {
                                                        workItem[externalCustomField] = customFieldValue;
                                                        changesMade = true;
                                                    }
                                                }
                                                else
                                                {
                                                    eventLog.WriteEntry("The custom property external key " + externalCustomField + " in project " + projectId + " does not exist in the list of TFS field definitions for this work item type!", EventLogEntryType.Warning);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return changesMade;
        }

        /// <summary>
        /// Adds tfs work item links to the Spira artifact
        /// </summary>
        /// <param name="links"></param>
        /// <param name="spiraImportExport"></param>
        /// <param name="artifactId"></param>
        /// <param name="artifactType"></param>
        private void ProcessTfsWorkItemLinks(LinkCollection links, ImportExportClient spiraImportExport, int artifactId, Constants.ArtifactType artifactType, RemoteDataMapping[] requirementMappings, RemoteDataMapping[] incidentMappings, RemoteDataMapping[] taskMappings)
        {
            try
            {
                //Loop through the links and add to spira as either associations or URL attachments
                foreach (Link link in links)
                {
                    //See if we have a URL or work item link, other types we cannot handle
                    if (link.BaseType == BaseLinkType.Hyperlink)
                    {
                        LogTraceEvent(String.Format("Adding TFS URL attachment to artifact id={0}, artifact type={1}", artifactId, artifactType.ToString()), EventLogEntryType.Information);

                        //Get the hyperlink URL
                        Hyperlink hyperlink = (Hyperlink)link;
                        string url = hyperlink.Location;
                        RemoteDocument remoteDocument = new RemoteDocument();
                        remoteDocument.FilenameOrUrl = url;
                        remoteDocument.ArtifactId = artifactId;
                        remoteDocument.ArtifactTypeId = (int)artifactType;
                        remoteDocument.Description = hyperlink.Comment;
                        spiraImportExport.Document_AddUrl(remoteDocument);
                    }
                    else if (link.BaseType == BaseLinkType.WorkItemLink)
                    {
                        LogTraceEvent(String.Format("Adding TFS work item link to artifact id={0}, artifact type={1}", artifactId, artifactType.ToString()), EventLogEntryType.Information);

                        //We need to get the destination artifact id and type from data mapping
                        RemoteAssociation remoteAssociation = new RemoteAssociation();
                        WorkItemLink workItemLink = (WorkItemLink)link;
                        string tfsDestWorkItemId = workItemLink.TargetId.ToString();
                        RemoteDataMapping dataMapping = InternalFunctions.FindMappingByExternalKey(tfsDestWorkItemId, requirementMappings);
                        bool matchFound = false;
                        if (dataMapping != null)
                        {
                            remoteAssociation.DestArtifactId = dataMapping.InternalId;
                            remoteAssociation.DestArtifactTypeId = (int)Constants.ArtifactType.Requirement;
                            matchFound = true;
                        }
                        dataMapping = InternalFunctions.FindMappingByExternalKey(tfsDestWorkItemId, incidentMappings);
                        if (dataMapping != null)
                        {
                            remoteAssociation.DestArtifactId = dataMapping.InternalId;
                            remoteAssociation.DestArtifactTypeId = (int)Constants.ArtifactType.Incident;
                            matchFound = true;
                        }
                        dataMapping = InternalFunctions.FindMappingByExternalKey(tfsDestWorkItemId, taskMappings);
                        if (dataMapping != null)
                        {
                            remoteAssociation.DestArtifactId = dataMapping.InternalId;
                            remoteAssociation.DestArtifactTypeId = (int)Constants.ArtifactType.Task;
                            matchFound = true;
                        }

                        //Create the link if a destination match was found
                        if (matchFound)
                        {
                            remoteAssociation.CreationDate = workItemLink.AddedDateUtc;
                            remoteAssociation.Comment = workItemLink.Comment;
                            remoteAssociation.SourceArtifactId = artifactId;
                            remoteAssociation.SourceArtifactTypeId = (int)artifactType;
                            spiraImportExport.Association_Create(remoteAssociation);
                        }
                    }
                    else if (link.BaseType == BaseLinkType.RelatedLink)
                    {
                        LogTraceEvent(String.Format("Adding TFS work item link to artifact id={0}, artifact type={1}", artifactId, artifactType.ToString()), EventLogEntryType.Information);

                        //We need to get the destination artifact id and type from data mapping
                        RemoteAssociation remoteAssociation = new RemoteAssociation();
                        RelatedLink relatedLink = (RelatedLink)link;
                        string tfsDestWorkItemId = relatedLink.RelatedWorkItemId.ToString();
                        RemoteDataMapping dataMapping = InternalFunctions.FindMappingByExternalKey(tfsDestWorkItemId, requirementMappings);
                        bool matchFound = false;
                        if (dataMapping != null)
                        {
                            remoteAssociation.DestArtifactId = dataMapping.InternalId;
                            remoteAssociation.DestArtifactTypeId = (int)Constants.ArtifactType.Requirement;
                            matchFound = true;
                        }
                        dataMapping = InternalFunctions.FindMappingByExternalKey(tfsDestWorkItemId, incidentMappings);
                        if (dataMapping != null)
                        {
                            remoteAssociation.DestArtifactId = dataMapping.InternalId;
                            remoteAssociation.DestArtifactTypeId = (int)Constants.ArtifactType.Incident;
                            matchFound = true;
                        }
                        dataMapping = InternalFunctions.FindMappingByExternalKey(tfsDestWorkItemId, taskMappings);
                        if (dataMapping != null)
                        {
                            remoteAssociation.DestArtifactId = dataMapping.InternalId;
                            remoteAssociation.DestArtifactTypeId = (int)Constants.ArtifactType.Task;
                            matchFound = true;
                        }

                        //Create the link if a destination match was found
                        if (matchFound)
                        {
                            remoteAssociation.CreationDate = DateTime.UtcNow;
                            remoteAssociation.Comment = relatedLink.Comment;
                            remoteAssociation.SourceArtifactId = artifactId;
                            remoteAssociation.SourceArtifactTypeId = (int)artifactType;
                            spiraImportExport.Association_Create(remoteAssociation);
                        }
                    }
                }
            }
            catch (Exception exception)
            {
                //Log error but continue
                LogErrorEvent(String.Format("Unable to add TFS work item link to artifact id={0}, artifact type={1}, message='{2}'", artifactId, artifactType.ToString(), exception.Message), EventLogEntryType.Error);
            }
        }

        /// <summary>
        /// Adds tfs work item attachments to the Spira artifact
        /// </summary>
        /// <param name="attachments"></param>
        /// <param name="spiraImportExport"></param>
        /// <param name="artifactId"></param>
        /// <param name="artifactType"></param>
        private void ProcessTfsWorkItemAttachments(AttachmentCollection attachments, ImportExportClient spiraImportExport, int artifactId, Constants.ArtifactType artifactType)
        {
            try
            {
                //Loop through the attachments and add to Spira
                foreach (Attachment attachment in attachments)
                {
                    //We need to download the attachment
                    string pathname = this.workItemServer.DownloadFile(attachment.Id);

                    //Now open up and read the bytes
                    byte[] binaryData = new byte[(int)attachment.Length];
                    using (FileStream fileStream = new FileStream(pathname, FileMode.Open, FileAccess.Read))
                    {
                        fileStream.Read(binaryData, 0, (int)attachment.Length);
                        fileStream.Close();
                    }

                    LogTraceEvent(String.Format("Adding TFS file attachment to artifact id={0}, artifact type={1}", artifactId, artifactType.ToString()), EventLogEntryType.Information);
                    RemoteDocument remoteDocument = new RemoteDocument();
                    remoteDocument.FilenameOrUrl = attachment.Name;
                    remoteDocument.ArtifactId = artifactId;
                    remoteDocument.ArtifactTypeId = (int)artifactType;
                    remoteDocument.Description = attachment.Comment;
                    remoteDocument.UploadDate = attachment.CreationTimeUtc;
                    spiraImportExport.Document_AddFile(remoteDocument, binaryData);

                    //Now delete the downloaded file
                    if (File.Exists(pathname))
                    {
                        File.Delete(pathname);
                    }
                }
            }
            catch (Exception exception)
            {
                //Log error but continue
                LogErrorEvent(String.Format("Unable to add TFS attachment to artifact id={0}, artifact type={1}, message='{2}'", artifactId, artifactType.ToString(), exception.Message), EventLogEntryType.Error);
            }
        }

        /// <summary>
        /// Updates the Spira custom properties with TFS field changes
        /// </summary>
        /// <returns>True if changes were made</returns>
        private bool ProcessWorkItemCustomFieldChanges(int projectId, WorkItem workItem, RemoteArtifact remoteArtifact, RemoteCustomProperty[] customProperties, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteDataMapping[] userMappings, ImportExportClient spiraImportExport, bool changesMade = false)
        {
            LogTraceEvent(eventLog, "Starting handling of work item mapped custom properties", EventLogEntryType.Information);

            try
            {
                //Loop through all the defined Spira custom properties
                foreach (SpiraImportExport.RemoteCustomProperty customProperty in customProperties)
                {
                    //Get the external key of this custom property (if it has one)
                    if (customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                    {
                        SpiraImportExport.RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                        if (customPropertyDataMapping != null)
                        {
                            LogTraceEvent(eventLog, "Found custom property mapping for TFS field " + customPropertyDataMapping.ExternalKey + "\n", EventLogEntryType.Information);
                            string externalKey = customPropertyDataMapping.ExternalKey;
                            //See if we have a list, multi-list or user custom field as they need to be handled differently
                            if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.List)
                            {
                                LogTraceEvent(eventLog, "TFS field " + customPropertyDataMapping.ExternalKey + " is mapped to a LIST property\n", EventLogEntryType.Information);

                                //First the single-list fields
                                if (externalKey == TFS_SPECIAL_FIELD_AREA)
                                {
                                    LogTraceEvent(eventLog, "TFS AreaId is mapped to LIST custom property " + customProperty.CustomPropertyFieldName + "\n", EventLogEntryType.Information);
                                    if (workItem.AreaId < 1)
                                    {
                                        changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (int?)null, changesMade);
                                    }
                                    else
                                    {
                                        //Now we need to set the value on the SpiraTest artifact - using the built-in TFS area field
                                        SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId.Value];
                                        SpiraImportExport.RemoteDataMapping customPropertyValueMapping = InternalFunctions.FindMappingByExternalKey(projectId, workItem.AreaId.ToString(), customPropertyValueMappings, false);
                                        if (customPropertyValueMapping != null)
                                        {
                                            changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, customPropertyValueMapping.InternalId, changesMade);
                                        }
                                    }
                                }
                                else
                                {
                                    //Now we need to set the value on the SpiraTest incident
                                    if (workItem.Fields.Contains(externalKey))
                                    {
                                        if (workItem[externalKey] == null)
                                        {
                                            changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (int?)null, changesMade);
                                        }
                                        else
                                        {
                                            //Need to get the Spira custom property value
                                            string fieldValue = workItem[externalKey].ToString();
                                            SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId.Value];
                                            SpiraImportExport.RemoteDataMapping customPropertyValueMapping = InternalFunctions.FindMappingByExternalKey(projectId, fieldValue, customPropertyValueMappings, false);
                                            if (customPropertyValueMapping != null)
                                            {
                                                changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, customPropertyValueMapping.InternalId, changesMade);
                                            }
                                        }
                                    }
                                    else
                                    {
                                        LogErrorEvent(String.Format("TFS work item doesn't have a field definition for '{0}'\n", externalKey), EventLogEntryType.Warning);
                                    }
                                }
                            }
                            else if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.User)
                            {
                                LogTraceEvent(eventLog, "TFS field " + customPropertyDataMapping.ExternalKey + " is mapped to a USER property\n", EventLogEntryType.Information);

                                //Now we need to set the value on the SpiraTest incident
                                if (workItem.Fields.Contains(externalKey))
                                {
                                    if (workItem[externalKey] == null)
                                    {
                                        changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (int?)null, changesMade);
                                    }
                                    else
                                    {
                                        //Need to get the Spira custom property value
                                        string fieldValue = workItem[externalKey].ToString();
                                        RemoteDataMapping customPropertyValueMapping = FindUserMappingByExternalKey(fieldValue, userMappings, spiraImportExport);
                                        if (customPropertyValueMapping != null)
                                        {
                                            changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, customPropertyValueMapping.InternalId, changesMade);
                                        }
                                    }
                                }
                                else
                                {
                                    LogErrorEvent(String.Format("TFS work item doesn't have a field definition for '{0}'\n", externalKey), EventLogEntryType.Warning);
                                }
                            }
                            else if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.MultiList)
                            {
                                LogTraceEvent(eventLog, "TFS field " + customPropertyDataMapping.ExternalKey + " is mapped to a MULTILIST property\n", EventLogEntryType.Information);

                                //Next the multi-list fields
                                //Now we need to set the value on the SpiraTest incident
                                if (workItem.Fields.Contains(externalKey))
                                {
                                    if (workItem[externalKey] == null)
                                    {
                                        changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (List<int>)null, changesMade);
                                    }
                                    else
                                    {
                                        //Need to get the Spira custom property value
                                        string fieldValueSerialized = workItem[externalKey].ToString();
                                        SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId.Value];

                                        //Data-map each of the custom property values
                                        List<int> spiraCustomValueIds = new List<int>();
                                        string[] ids = fieldValueSerialized.Split(';'); //TFS separates the IDs by string
                                        foreach (string id in ids)
                                        {
                                            RemoteDataMapping customPropertyValueMapping = InternalFunctions.FindMappingByExternalKey(projectId, id, customPropertyValueMappings, false);
                                            if (customPropertyValueMapping != null)
                                            {
                                                spiraCustomValueIds.Add(customPropertyValueMapping.InternalId);
                                            }
                                        }
                                        changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, spiraCustomValueIds, changesMade);
                                    }
                                }
                                else
                                {
                                    LogErrorEvent(String.Format("TFS work item doesn't have a field definition for '{0}'\n", externalKey), EventLogEntryType.Warning);
                                }
                            }
                            else
                            {
                                LogTraceEvent(eventLog, "TFS field " + customPropertyDataMapping.ExternalKey + " is mapped to a VALUE property\n", EventLogEntryType.Information);

                                //See if we have any special TFS fields
                                if (externalKey == TFS_SPECIAL_FIELD_WORK_ITEM_ID)
                                {
                                    //Now we need to set the work item id on the SpiraTest incident
                                    LogTraceEvent(eventLog, "Setting TFS work item id '" + workItem.Id + "' on  artifact custom property\n", EventLogEntryType.Information);
                                    changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, workItem.Id.ToString(), changesMade);
                                }
                                else
                                {
                                    //Now we need to set the value on the SpiraTest artifact
                                    if (workItem.Fields.Contains(externalKey))
                                    {
                                        if (workItem[externalKey] != null)
                                        {
                                            LogTraceEvent(eventLog, String.Format("The '{0}' field on the TFS work item id is of type: {1}'", externalKey, workItem[externalKey].GetType().Name), EventLogEntryType.Information);
                                        }
                                        switch ((Constants.CustomPropertyType)customProperty.CustomPropertyTypeId)
                                        {
                                            case Constants.CustomPropertyType.Boolean:
                                                {
                                                    if (workItem[externalKey] == null)
                                                    {
                                                        changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (bool?)null, changesMade);
                                                    }
                                                    else
                                                    {
                                                        bool boolValue;
                                                        if (workItem[externalKey] is Boolean)
                                                        {
                                                            changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (bool)workItem[externalKey], changesMade);
                                                            LogTraceEvent(eventLog, "Setting TFS field " + customPropertyDataMapping.ExternalKey + " value '" + workItem[externalKey] + "' on artifact\n", EventLogEntryType.Information);
                                                        }
                                                        else if (workItem[externalKey] is String && Boolean.TryParse((string)workItem[externalKey], out boolValue))
                                                        {
                                                            changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, boolValue, changesMade);
                                                            LogTraceEvent(eventLog, "Setting TFS field " + customPropertyDataMapping.ExternalKey + " value '" + workItem[externalKey] + "' on artifact\n", EventLogEntryType.Information);
                                                        }
                                                    }
                                                }
                                                break;

                                            case Constants.CustomPropertyType.Date:
                                                {
                                                    if (workItem[externalKey] == null)
                                                    {
                                                        changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (DateTime?)null, changesMade);
                                                    }
                                                    else
                                                    {
                                                        DateTime dateTimeValue;
                                                        if (workItem[externalKey] is DateTime)
                                                        {
                                                            //Need to convert to UTC for Spira
                                                            DateTime localTime = (DateTime)workItem[externalKey];
                                                            DateTime utcTime = localTime.ToUniversalTime();

                                                            changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, utcTime, changesMade);
                                                            LogTraceEvent(eventLog, "Setting TFS field " + customPropertyDataMapping.ExternalKey + " value '" + utcTime + "' on artifact\n", EventLogEntryType.Information);
                                                        }
                                                        else if (workItem[externalKey] is String && DateTime.TryParse((string)workItem[externalKey], out dateTimeValue))
                                                        {
                                                            //Need to convert to UTC for Spira
                                                            DateTime utcTime = dateTimeValue.ToUniversalTime();

                                                            changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, utcTime, changesMade);
                                                            LogTraceEvent(eventLog, "Setting TFS field " + customPropertyDataMapping.ExternalKey + " value '" + utcTime + "' on artifact\n", EventLogEntryType.Information);
                                                        }
                                                    }
                                                }
                                                break;


                                            case Constants.CustomPropertyType.Decimal:
                                                {
                                                    if (workItem[externalKey] == null)
                                                    {
                                                        changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (decimal?)null, changesMade);
                                                    }
                                                    else
                                                    {
                                                        Decimal decimalValue;
                                                        if (workItem[externalKey] is Decimal)
                                                        {
                                                            changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (decimal)workItem[externalKey], changesMade);
                                                            LogTraceEvent(eventLog, "Setting TFS field " + customPropertyDataMapping.ExternalKey + " value '" + workItem[externalKey] + "' on artifact\n", EventLogEntryType.Information);
                                                        }
                                                        else if (workItem[externalKey] is Double)
                                                        {
                                                            //Convert from double to decimal
                                                            double dblValue = (double)workItem[externalKey];
                                                            changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (decimal)dblValue, changesMade);
                                                            LogTraceEvent(eventLog, "Setting TFS field " + customPropertyDataMapping.ExternalKey + " value '" + workItem[externalKey] + "' on artifact\n", EventLogEntryType.Information);
                                                        }
                                                        else if (workItem[externalKey] is String && Decimal.TryParse((string)workItem[externalKey], out decimalValue))
                                                        {
                                                            changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, decimalValue, changesMade);
                                                            LogTraceEvent(eventLog, "Setting TFS field " + customPropertyDataMapping.ExternalKey + " value '" + workItem[externalKey] + "' on artifact\n", EventLogEntryType.Information);
                                                        }
                                                    }
                                                }
                                                break;

                                            case Constants.CustomPropertyType.Integer:
                                                {
                                                    if (workItem[externalKey] == null)
                                                    {
                                                        changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (int?)null, changesMade);
                                                    }
                                                    else
                                                    {
                                                        Int32 intValue;
                                                        if (workItem[externalKey] is Int32)
                                                        {
                                                            changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (int)workItem[externalKey], changesMade);
                                                            LogTraceEvent(eventLog, "Setting TFS field " + customPropertyDataMapping.ExternalKey + " value '" + workItem[externalKey] + "' on artifact\n", EventLogEntryType.Information);
                                                        }
                                                        else if (workItem[externalKey] is String && Int32.TryParse((string)workItem[externalKey], out intValue))
                                                        {
                                                            changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, intValue, changesMade);
                                                            LogTraceEvent(eventLog, "Setting TFS field " + customPropertyDataMapping.ExternalKey + " value '" + workItem[externalKey] + "' on artifact\n", EventLogEntryType.Information);
                                                        }
                                                    }
                                                }
                                                break;

                                            case Constants.CustomPropertyType.Text:
                                            default:
                                                {
                                                    if (workItem[externalKey] == null)
                                                    {
                                                        changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (string)null, changesMade);
                                                    }
                                                    else
                                                    {
                                                        changesMade = InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, workItem[externalKey].ToString(), changesMade);
                                                        LogTraceEvent(eventLog, "Setting TFS field " + customPropertyDataMapping.ExternalKey + " value '" + workItem[externalKey].ToString() + "' on artifact\n", EventLogEntryType.Information);
                                                    }
                                                }
                                                break;
                                        }
                                    }
                                    else
                                    {
                                        LogErrorEvent(String.Format("TFS work item doesn't have a field definition for '{0}'\n", externalKey), EventLogEntryType.Warning);
                                    }
                                }
                            }
                        }
                    }
                }

                LogTraceEvent(eventLog, "Finished handling of work item mapped custom properties", EventLogEntryType.Information);
            }
            catch (Exception exception)
            {
                LogErrorEvent("Error handling work item custom fields, some field values may not be set correctly. Error = " + exception.Message, EventLogEntryType.Error);
            }

            return changesMade;
        }
    }
}

