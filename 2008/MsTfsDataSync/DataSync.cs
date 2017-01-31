using System;
using System.Web;
using System.Web.Services;
using System.Web.Services.Protocols;
using System.Diagnostics;
using System.Collections;
using System.Net;
using System.Collections.Generic;
using System.Configuration;
using System.Xml;
using System.Text;
using System.Threading;

using Inflectra.SpiraTest.PlugIns.MsTfsDataSync.SpiraImportExport;

using Microsoft.TeamFoundation;
using Microsoft.TeamFoundation.Client;
using Microsoft.TeamFoundation.Common;
using Microsoft.TeamFoundation.Server;
using Microsoft.TeamFoundation.WorkItemTracking;
using Microsoft.TeamFoundation.WorkItemTracking.Client;

namespace Inflectra.SpiraTest.PlugIns.MsTfsDataSync
{
	/// <summary>
	/// Contains all the logic necessary to sync SpiraTest with Microsoft Team Foundation Server
	/// </summary>
	public class DataSync : IServicePlugIn
	{
        //Constant containing data-sync name and internal API URL suffix to access
        private const string DATA_SYNC_NAME = "MsTfsDataSync";
        private const string WEB_SERVICE_URL_SUFFIX = "/Services/v2_2/ImportExport.asmx";

        //Certain local constants
        private const int ARTIFACT_TYPE_ID_RELEASE = 4;
        private const int ARTIFACT_TYPE_ID_INCIDENT = 3;
        private const int ARTIFACT_TYPE_ID_TASK = 6;
        private const int ARTIFACT_FIELD_ID_INCIDENT_SEVERITY = 1;
        private const int ARTIFACT_FIELD_ID_INCIDENT_PRIORITY = 2;
        private const int ARTIFACT_FIELD_ID_INCIDENT_STATUS = 3;
        private const int ARTIFACT_FIELD_ID_INCIDENT_TYPE = 4;
        private const int ARTIFACT_FIELD_ID_TASK_PRIORITY = 59;
        private const int ARTIFACT_FIELD_ID_TASK_STATUS = 57;
        private const int CUSTOM_PROPERTY_TYPE_TEXT = 1;
        private const int CUSTOM_PROPERTY_TYPE_LIST = 2;
        private const string NEW_TASK_NAME_TO_IGNORE = "New Task";

        //Special TFS fields that we map to Spira custom properties
        private const string TFS_SPECIAL_FIELD_RANK = "Rank";
        private const string TFS_SPECIAL_FIELD_TRIAGE = "Triage";
        private const string TFS_SPECIAL_FIELD_AREA = "Area";
        private const string TFS_SPECIAL_FIELD_DISCIPLINE = "Discipline";

        //Artifact prefixes
        private const string ARTIFACT_TYPE_PREFIX_INCIDENT = "IN";
        private const string ARTIFACT_TYPE_PREFIX_TASK = "TK";

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
        private string syncOnlyType;
        private string artifactIdTfsField;
        private string incidentDetectorTfsField;

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
        /// <param name="custom02">Should we sync only tasks or only incidents</param>
        /// <param name="custom03">Not used by this plug-in</param>
        /// <param name="custom04">Not used by this plug-in</param>
        /// <param name="custom05">Not used by this plug-in</param>
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
                this.syncOnlyType = custom02;
                this.artifactIdTfsField = custom03;
                this.incidentDetectorTfsField = custom04;
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
        /// <param name="LastSyncDate">The last date/time the plug-in was successfully executed</param>
        /// <param name="serverDateTime">The current date/time on the server</param>
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
                SpiraImportExport.ImportExport spiraImportExport = new SpiraImportExport.ImportExport();
                spiraImportExport.Url = this.webServiceBaseUrl + WEB_SERVICE_URL_SUFFIX;

				//Create new cookie container to hold the session handles
				CookieContainer cookieContainer = new CookieContainer();
                spiraImportExport.CookieContainer = cookieContainer;

                //First lets get the product name we should be referring to
                string productName = spiraImportExport.System_GetProductName();

                //**** Next lets load in the project and user mappings ****
                bool success = spiraImportExport.Connection_Authenticate(internalLogin, internalPassword);
                if (!success)
                {
                    //We can't authenticate so end
                    eventLog.WriteEntry("Unable to authenticate with " + productName + " API, stopping data-synchronization", EventLogEntryType.Error);
                    return ServiceReturnType.Error;
                }
                SpiraImportExport.RemoteDataMapping[] projectMappings = spiraImportExport.DataMapping_RetrieveProjectMappings(dataSyncSystemId);
                SpiraImportExport.RemoteDataMapping[] userMappings = spiraImportExport.DataMapping_RetrieveUserMappings(dataSyncSystemId);

                //Configure the network credentials - used for accessing the MsTfs API
                ICredentials credentials = new NetworkCredential(this.externalLogin, this.externalPassword, this.windowsDomain);

                //Create a new TeamFoundationServer instance and WorkItemStore instance
                //Using TeamFoundationServerFactory seemed to cause memory leaks over time
                WorkItemStore workItemStore = null;
                TeamFoundationServer teamFoundationServer = new TeamFoundationServer(this.connectionString, credentials);
                //Get access to the work item store
                try
                {
                    workItemStore = new WorkItemStore(teamFoundationServer);
                }
                catch (Exception exception)
                {
                    //We can't authenticate so end
                    eventLog.WriteEntry("Unable to connect to Team Foundation Server, please check that the connection information is correct (" + exception.Message + ")", EventLogEntryType.Error);
                    return ServiceReturnType.Error;
                }
                if (workItemStore == null)
                {
                    //We can't authenticate so end
                    eventLog.WriteEntry("Unable to connect to Team Foundation Server, please check that the connection information is correct", EventLogEntryType.Error);
                    return ServiceReturnType.Error;
                }

				//Loop for each of the projects in the project mapping
                SpiraImportExport.RemoteDataMapping dataMapping; 
                foreach (SpiraImportExport.RemoteDataMapping projectMapping in projectMappings)
				{
					//Get the SpiraTest project id equivalent TFS project identifier
                    int projectId = projectMapping.InternalId;
                    string tfsProject = projectMapping.ExternalKey;

					//Connect to the SpiraTest project
                    success = spiraImportExport.Connection_ConnectToProject(projectId);
                    if (!success)
                    {
                        //We can't connect so go to next project
                        eventLog.WriteEntry("Unable to connect to " + productName + " project, please check that the " + productName + " login has the appropriate permissions", EventLogEntryType.Error);
                        continue;
                    }

                    //Connect to the TFS project
                    Project project = workItemStore.Projects[tfsProject];

                    //Get the list of project-specific mappings from the data-mapping repository
                    SpiraImportExport.RemoteDataMapping[] incidentSeverityMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, ARTIFACT_FIELD_ID_INCIDENT_SEVERITY);
                    SpiraImportExport.RemoteDataMapping[] incidentPriorityMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, ARTIFACT_FIELD_ID_INCIDENT_PRIORITY);
                    SpiraImportExport.RemoteDataMapping[] incidentStatusMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, ARTIFACT_FIELD_ID_INCIDENT_STATUS);
                    SpiraImportExport.RemoteDataMapping[] incidentTypeMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, ARTIFACT_FIELD_ID_INCIDENT_TYPE);
                    SpiraImportExport.RemoteDataMapping[] taskPriorityMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, ARTIFACT_FIELD_ID_TASK_PRIORITY);
                    SpiraImportExport.RemoteDataMapping[] taskStatusMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, ARTIFACT_FIELD_ID_TASK_STATUS);

                    //Get the list of custom properties configured for this project and the corresponding data mappings
                    //First for incidents
                    SpiraImportExport.RemoteCustomProperty[] incidentProjectCustomProperties = spiraImportExport.CustomProperty_RetrieveProjectProperties(ARTIFACT_TYPE_ID_INCIDENT);
                    Dictionary<int, SpiraImportExport.RemoteDataMapping> incidentCustomPropertyMappingList = new Dictionary<int, SpiraImportExport.RemoteDataMapping>();
                    Dictionary<int, SpiraImportExport.RemoteDataMapping[]> incidentCustomPropertyValueMappingList = new Dictionary<int, SpiraImportExport.RemoteDataMapping[]>();
                    foreach (SpiraImportExport.RemoteCustomProperty customProperty in incidentProjectCustomProperties)
                    {
                        //Get the mapping for this custom property
                        SpiraImportExport.RemoteDataMapping customPropertyMapping = spiraImportExport.DataMapping_RetrieveCustomPropertyMapping(dataSyncSystemId, ARTIFACT_TYPE_ID_INCIDENT, customProperty.CustomPropertyId);
                        incidentCustomPropertyMappingList.Add(customProperty.CustomPropertyId, customPropertyMapping);

                        //For list types need to also get the property value mappings
                        if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_LIST)
                        {
                            SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = spiraImportExport.DataMapping_RetrieveCustomPropertyValueMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_INCIDENT, customProperty.CustomPropertyId);
                            incidentCustomPropertyValueMappingList.Add(customProperty.CustomPropertyId, customPropertyValueMappings);
                        }
                    }

                    //Next for tasks
                    SpiraImportExport.RemoteCustomProperty[] taskProjectCustomProperties = spiraImportExport.CustomProperty_RetrieveProjectProperties(ARTIFACT_TYPE_ID_TASK);
                    Dictionary<int, SpiraImportExport.RemoteDataMapping> taskCustomPropertyMappingList = new Dictionary<int, SpiraImportExport.RemoteDataMapping>();
                    Dictionary<int, SpiraImportExport.RemoteDataMapping[]> taskCustomPropertyValueMappingList = new Dictionary<int, SpiraImportExport.RemoteDataMapping[]>();
                    foreach (SpiraImportExport.RemoteCustomProperty customProperty in taskProjectCustomProperties)
                    {
                        //Get the mapping for this custom property
                        SpiraImportExport.RemoteDataMapping customPropertyMapping = spiraImportExport.DataMapping_RetrieveCustomPropertyMapping(dataSyncSystemId, ARTIFACT_TYPE_ID_TASK, customProperty.CustomPropertyId);
                        taskCustomPropertyMappingList.Add(customProperty.CustomPropertyId, customPropertyMapping);

                        //For list types need to also get the property value mappings
                        if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_LIST)
                        {
                            SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = spiraImportExport.DataMapping_RetrieveCustomPropertyValueMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_TASK, customProperty.CustomPropertyId);
                            taskCustomPropertyValueMappingList.Add(customProperty.CustomPropertyId, customPropertyValueMappings);
                        }
                    }

                    //Now get the list of releases, tasks and incidents that have already been mapped
                    SpiraImportExport.RemoteDataMapping[] incidentMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_INCIDENT);
                    SpiraImportExport.RemoteDataMapping[] taskMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_TASK);
                    SpiraImportExport.RemoteDataMapping[] releaseMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_RELEASE);

                    //Create the mapping collections to hold any new items that need to get added to the mappings
                    //or any old items that need to get removed from the mappings
                    List<SpiraImportExport.RemoteDataMapping> newIncidentMappings = new List<SpiraImportExport.RemoteDataMapping>();
                    List<SpiraImportExport.RemoteDataMapping> newTaskMappings = new List<SpiraImportExport.RemoteDataMapping>();
                    List<SpiraImportExport.RemoteDataMapping> newReleaseMappings = new List<SpiraImportExport.RemoteDataMapping>();

                    //**** First we need to get the list of recently created incidents in SpiraTest ****
                    if (!lastSyncDate.HasValue)
                    {
                        lastSyncDate = DateTime.Parse("1/1/1900");
                    }

                    if (String.IsNullOrEmpty(this.syncOnlyType) || this.syncOnlyType.ToLowerInvariant() == "incidents")
                    {
                        SpiraImportExport.RemoteIncident[] incidentList = spiraImportExport.Incident_RetrieveNew(lastSyncDate.Value);

                        //Iterate through each record
                        foreach (SpiraImportExport.RemoteIncident remoteIncident in incidentList)
                        {
                            try
                            {
                                //Get certain incident fields into local variables (if used more than once)
                                int incidentId = remoteIncident.IncidentId.Value;
                                int incidentStatusId = remoteIncident.IncidentStatusId;

                                //Make sure we've not already loaded this incident
                                if (FindMappingByInternalId(projectId, incidentId, incidentMappings) == null)
                                {
                                    //Now get the work item type from the mapping
                                    //Tasks are handled separately unless they are mapped, need to check
                                    dataMapping = FindMappingByInternalId(projectId, remoteIncident.IncidentTypeId, incidentTypeMappings);
                                    if (dataMapping == null)
                                    {
                                        //We can't find the matching item so log and move to the next incident
                                        eventLog.WriteEntry("Unable to locate mapping entry for incident type " + remoteIncident.IncidentTypeId + " in project " + projectId, EventLogEntryType.Error);
                                        continue;
                                    }
                                    string workItemTypeName = dataMapping.ExternalKey;

                                    //First we need to get the Iteration, mapped from the SpiraTest Release, if not create it
                                    //Need to do this before creating the work item as we may need to reload the project reference
                                    int iterationId = -1;
                                    if (remoteIncident.DetectedReleaseId.HasValue)
                                    {
                                        int detectedReleaseId = remoteIncident.DetectedReleaseId.Value;
                                        dataMapping = FindMappingByInternalId(projectId, detectedReleaseId, releaseMappings);
                                        if (dataMapping == null)
                                        {
                                            //Now check to see if recently added
                                            dataMapping = FindMappingByInternalId(projectId, detectedReleaseId, newReleaseMappings.ToArray());
                                        }
                                        if (dataMapping == null)
                                        {
                                            //We can't find the matching item so need to create a new iteration in TFS and add to mappings
                                            LogTraceEvent(eventLog, "Adding new iteration in TFS for release " + detectedReleaseId + "\n", EventLogEntryType.Information);
                                            Node newIterationNode = AddNewTfsIteration(teamFoundationServer, ref workItemStore, ref project, remoteIncident.DetectedReleaseVersionNumber);

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
                                    WorkItemType workItemType = project.WorkItemTypes[workItemTypeName];
                                    WorkItem workItem = new WorkItem(workItemType);
                                    workItem.Title = remoteIncident.Name;
                                    workItem.Description = "Incident IN" + incidentId.ToString() + " detected by " + remoteIncident.OpenerName + " in " + productName + ". \n" + HtmlRenderAsPlainText(remoteIncident.Description);
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
                                        workItem[this.artifactIdTfsField] = ARTIFACT_TYPE_PREFIX_INCIDENT + remoteIncident.IncidentId.Value;
                                    }
                                    if (!String.IsNullOrEmpty(this.incidentDetectorTfsField) && workItem.Type.FieldDefinitions.Contains(this.incidentDetectorTfsField))
                                    {
                                        workItem[this.incidentDetectorTfsField] = remoteIncident.OpenerName;
                                    }

                                    //We have to always initially create the work items in their default state
                                    //So changes to the State + Reason have to come later

                                    //Now get the incident status from the mapping
                                    dataMapping = FindMappingByInternalId(projectId, remoteIncident.IncidentStatusId, incidentStatusMappings);
                                    if (dataMapping == null)
                                    {
                                        //We can't find the matching item so log and move to the next incident
                                        eventLog.WriteEntry("Unable to locate mapping entry for incident status " + remoteIncident.IncidentStatusId + " in project " + projectId, EventLogEntryType.Error);
                                        continue;
                                    }
                                    //The status in SpiraTest = MSTFS State+Reason
                                    string[] stateAndReason = dataMapping.ExternalKey.Split('+');
                                    string tfsState = stateAndReason[0];
                                    string tfsReason = stateAndReason[1];

                                    //Now get the incident priority from the mapping (if priority is set)
                                    try
                                    {
                                        if (remoteIncident.PriorityId.HasValue)
                                        {
                                            dataMapping = FindMappingByInternalId(projectId, remoteIncident.PriorityId.Value, incidentPriorityMappings);
                                            if (dataMapping == null)
                                            {
                                                //We can't find the matching item so log and just don't set the priority
                                                eventLog.WriteEntry("Unable to locate mapping entry for incident priority " + remoteIncident.PriorityId.Value + " in project " + projectId, EventLogEntryType.Warning);
                                            }
                                            else
                                            {
                                                if (workItem.Fields.Contains("Priority") && workItem.Fields["Priority"].IsValid)
                                                {
                                                    workItem["Priority"] = dataMapping.ExternalKey;
                                                }
                                            }
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

                                    //The creator is not allowed to be set on the work-item (read-only)

                                    //Now set the assignee
                                    if (remoteIncident.OwnerId.HasValue)
                                    {
                                        dataMapping = FindMappingByInternalId(remoteIncident.OwnerId.Value, userMappings);
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

                                    //Now iterate through the project custom properties
                                    foreach (SpiraImportExport.RemoteCustomProperty customProperty in incidentProjectCustomProperties)
                                    {
                                        //Handle list and text ones separately
                                        if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_TEXT)
                                        {
                                            //See if we have a custom property value set
                                            String customPropertyValue = GetCustomPropertyTextValue(remoteIncident, customProperty.CustomPropertyName);
                                            if (!String.IsNullOrEmpty(customPropertyValue))
                                            {
                                                //Get the corresponding external custom field (if there is one)
                                                if (incidentCustomPropertyMappingList.ContainsKey(customProperty.CustomPropertyId))
                                                {
                                                    string externalCustomField = incidentCustomPropertyMappingList[customProperty.CustomPropertyId].ExternalKey;

                                                    //See if we have one of the special standard TFS field that it maps to
                                                    if (workItem.Fields.Contains("Rank") && externalCustomField == TFS_SPECIAL_FIELD_RANK)
                                                    {
                                                        workItem["Rank"] = customPropertyValue;
                                                    }
                                                    else if (workItem.Fields.Contains(externalCustomField))
                                                    {
                                                        //This needs to be added to the list of TFS custom properties
                                                        workItem[externalCustomField] = customPropertyValue;
                                                    }
                                                }
                                            }
                                        }
                                        if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_LIST)
                                        {
                                            //See if we have a custom property value set
                                            Nullable<int> customPropertyValue = GetCustomPropertyListValue(remoteIncident, customProperty.CustomPropertyName);

                                            //Get the corresponding external custom field (if there is one)
                                            if (customPropertyValue.HasValue && incidentCustomPropertyMappingList.ContainsKey(customProperty.CustomPropertyId))
                                            {
                                                string externalCustomField = incidentCustomPropertyMappingList[customProperty.CustomPropertyId].ExternalKey;

                                                //Get the corresponding external custom field value (if there is one)
                                                if (incidentCustomPropertyValueMappingList.ContainsKey(customProperty.CustomPropertyId))
                                                {
                                                    SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = incidentCustomPropertyValueMappingList[customProperty.CustomPropertyId];
                                                    SpiraImportExport.RemoteDataMapping customPropertyValueMapping = FindMappingByInternalId(projectId, customPropertyValue.Value, customPropertyValueMappings);
                                                    if (customPropertyValueMapping != null)
                                                    {
                                                        string externalCustomFieldValue = customPropertyValueMapping.ExternalKey;

                                                        //See if we have one of the special standard TFS fields that it maps to
                                                        if (externalCustomField == TFS_SPECIAL_FIELD_AREA)
                                                        {
                                                            //Now set the value of the work item's area
                                                            int areaId = -1;
                                                            if (Int32.TryParse(externalCustomFieldValue, out areaId))
                                                            {
                                                                workItem.AreaId = areaId;
                                                            }
                                                            else
                                                            {
                                                                eventLog.WriteEntry("The area external key " + externalCustomFieldValue + " in project " + projectId + " is invalid - it needs to be numeric!", EventLogEntryType.Warning);
                                                            }
                                                        }
                                                        else if (externalCustomField == TFS_SPECIAL_FIELD_TRIAGE)
                                                        {
                                                            //Now set the value of the work item's triage status
                                                            if (workItem.Fields.Contains("Triage"))
                                                            {
                                                                workItem["Triage"] = externalCustomFieldValue;
                                                            }
                                                        }
                                                        else if (workItem.Fields.Contains(externalCustomField))
                                                        {
                                                            //This needs to be added to the list of TFS custom properties
                                                            workItem[externalCustomField] = externalCustomFieldValue;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

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
                                                workItem.Save();
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

                                        //Finally add any resolutions as history items to the work item if appropriate
                                        RemoteIncidentResolution[] remoteResolutions = spiraImportExport.Incident_RetrieveResolutions(incidentId);
                                        foreach (RemoteIncidentResolution remoteResolution in remoteResolutions)
                                        {
                                            workItem.History = HtmlRenderAsPlainText(remoteResolution.Resolution);
                                            workItem.Save();
                                        }
                                    }
                                    else
                                    {
                                        //Log the detailed error message
                                        eventLog.WriteEntry("Error Adding " + productName + " Incident to Team Foundation Server: " + messages.ToString(), EventLogEntryType.Error);
                                    }
                                }
                            }
                            catch (Exception exception)
                            {
                                //Log and continue execution
                                eventLog.WriteEntry("Error Adding " + productName + " Incident to Team Foundation Server: " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                            }
                        }
                        //Finally we need to update the mapping data on the server before starting the second phase
                        //of the data-synchronization
                        //At this point we have potentially added incidents
                        spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_INCIDENT, newIncidentMappings.ToArray());
                        incidentList = null;
                    }

                    //**** Next we need to get the list of recently created tasks in SpiraTeam ****
                    if (!lastSyncDate.HasValue)
                    {
                        lastSyncDate = DateTime.Parse("1/1/1900");
                    }
                    if (String.IsNullOrEmpty(this.syncOnlyType) || this.syncOnlyType.ToLowerInvariant() == "tasks")
                    {
                        SpiraImportExport.RemoteTask[] taskList = spiraImportExport.Task_RetrieveNew(lastSyncDate.Value);

                        //Iterate through each record
                        foreach (SpiraImportExport.RemoteTask remoteTask in taskList)
                        {
                            try
                            {
                                //Get certain task fields into local variables (if used more than once)
                                int taskId = remoteTask.TaskId.Value;
                                int taskStatusId = remoteTask.TaskStatusId;

                                //Make sure we've not already loaded this task
                                //Also ignore any tasks called 'New Task' as they haven't have their details entered yet
                                //and can be considered incomplete
                                if (FindMappingByInternalId(projectId, taskId, taskMappings) == null && remoteTask.Name.Trim() != NEW_TASK_NAME_TO_IGNORE)
                                {
                                    //Note: Tasks always map to the Task Work Item Type in TFS

                                    //First we need to get the Iteration, mapped from the SpiraTest Release, if not create it
                                    //Need to do this before creating the work item as we may need to reload the project reference
                                    int iterationId = -1;
                                    if (remoteTask.ReleaseId.HasValue)
                                    {
                                        int detectedReleaseId = remoteTask.ReleaseId.Value;
                                        dataMapping = FindMappingByInternalId(projectId, detectedReleaseId, releaseMappings);
                                        if (dataMapping == null)
                                        {
                                            //Now check to see if recently added
                                            dataMapping = FindMappingByInternalId(projectId, detectedReleaseId, newReleaseMappings.ToArray());
                                        }
                                        if (dataMapping == null)
                                        {
                                            //We can't find the matching item so need to create a new iteration in TFS and add to mappings
                                            LogTraceEvent(eventLog, "Adding new iteration in TFS for release " + detectedReleaseId + "\n", EventLogEntryType.Information);
                                            Node newIterationNode = AddNewTfsIteration(teamFoundationServer, ref workItemStore, ref project, remoteTask.ReleaseVersionNumber);

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
                                    WorkItemType workItemType = project.WorkItemTypes["Task"];
                                    WorkItem workItem = new WorkItem(workItemType);
                                    workItem.Title = remoteTask.Name;
                                    workItem.Description = HtmlRenderAsPlainText(remoteTask.Description);

                                    //See if we need to populate TFS custom fields with the Spira task ID
                                    if (!String.IsNullOrEmpty(this.artifactIdTfsField) && workItem.Type.FieldDefinitions.Contains(this.artifactIdTfsField))
                                    {
                                        workItem[this.artifactIdTfsField] = ARTIFACT_TYPE_PREFIX_TASK + remoteTask.TaskId.Value;
                                    }

                                    if (iterationId != -1)
                                    {
                                        workItem.IterationId = iterationId;
                                    }
                                    try
                                    {
                                        if (remoteTask.StartDate.HasValue)
                                        {
                                            workItem["Start Date"] = remoteTask.StartDate;
                                        }
                                        if (remoteTask.EndDate.HasValue)
                                        {
                                            workItem["Finish Date"] = remoteTask.StartDate.Value;
                                        }
                                    }
                                    catch (Exception)
                                    {
                                        //Some installations won't let the dates be set
                                    }

                                    if (remoteTask.ActualEffort.HasValue)
                                    {
                                        double completedWork = (double)remoteTask.ActualEffort.Value / (double)60;
                                        workItem["Completed Work"] = completedWork;
                                        if (remoteTask.EstimatedEffort.HasValue)
                                        {
                                            double remainingWork = (double)(remoteTask.EstimatedEffort.Value - remoteTask.ActualEffort.Value) / (double)60;
                                            if (remainingWork < 0)
                                            {
                                                remainingWork = 0;
                                            }
                                            workItem["Remaining Work"] = remainingWork;
                                        }
                                    }

                                    //Add a link to the SpiraTest task
                                    string taskUrl = spiraImportExport.System_GetWebServerUrl() + "/TaskDetails.aspx?taskId=" + taskId.ToString();
                                    workItem.Links.Add(new Hyperlink(taskUrl));

                                    //We have to always initially create the work items in their default state
                                    //So changes to the State have to come later

                                    //Now get the task status from the mapping
                                    dataMapping = FindMappingByInternalId(projectId, remoteTask.TaskStatusId, taskStatusMappings);
                                    if (dataMapping == null)
                                    {
                                        //We can't find the matching item so log and move to the next task
                                        eventLog.WriteEntry("Unable to locate mapping entry for task status " + remoteTask.TaskStatusId + " in project " + projectId, EventLogEntryType.Error);
                                        continue;
                                    }
                                    //The status in SpiraTest = MSTFS State only for tasks
                                    string tfsState = dataMapping.ExternalKey;

                                    //The creator is not allowed to be set on the work-item (read-only)

                                    //Now set the assignee
                                    if (remoteTask.OwnerId.HasValue)
                                    {
                                        dataMapping = FindMappingByInternalId(remoteTask.OwnerId.Value, userMappings);
                                        if (dataMapping == null)
                                        {
                                            //We can't find the matching user so ignore
                                            eventLog.WriteEntry("Unable to locate mapping entry for user id " + remoteTask.OwnerId.Value + " so leaving blank", EventLogEntryType.Warning);
                                        }
                                        else
                                        {
                                            workItem[CoreField.AssignedTo] = dataMapping.ExternalKey;
                                        }
                                    }

                                    //Now iterate through the project custom properties
                                    foreach (SpiraImportExport.RemoteCustomProperty customProperty in taskProjectCustomProperties)
                                    {
                                        //Handle list and text ones separately
                                        if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_TEXT)
                                        {
                                            //See if we have a custom property value set
                                            String customPropertyValue = GetCustomPropertyTextValue(remoteTask, customProperty.CustomPropertyName);
                                            if (!String.IsNullOrEmpty(customPropertyValue))
                                            {
                                                //Get the corresponding external custom field (if there is one)
                                                if (taskCustomPropertyMappingList.ContainsKey(customProperty.CustomPropertyId))
                                                {
                                                    string externalCustomField = taskCustomPropertyMappingList[customProperty.CustomPropertyId].ExternalKey;

                                                    //See if we have one of the special standard TFS field that it maps to
                                                    if (externalCustomField == TFS_SPECIAL_FIELD_RANK)
                                                    {
                                                        workItem["Rank"] = customPropertyValue;
                                                    }
                                                    else
                                                    {
                                                        //This needs to be added to the list of TFS custom properties
                                                        workItem[externalCustomField] = customPropertyValue;
                                                    }
                                                }
                                            }
                                        }
                                        if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_LIST)
                                        {
                                            //See if we have a custom property value set
                                            Nullable<int> customPropertyValue = GetCustomPropertyListValue(remoteTask, customProperty.CustomPropertyName);

                                            //Get the corresponding external custom field (if there is one)
                                            if (customPropertyValue.HasValue && taskCustomPropertyMappingList.ContainsKey(customProperty.CustomPropertyId))
                                            {
                                                string externalCustomField = taskCustomPropertyMappingList[customProperty.CustomPropertyId].ExternalKey;

                                                //Get the corresponding external custom field value (if there is one)
                                                if (taskCustomPropertyValueMappingList.ContainsKey(customProperty.CustomPropertyId))
                                                {
                                                    SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = taskCustomPropertyValueMappingList[customProperty.CustomPropertyId];
                                                    SpiraImportExport.RemoteDataMapping customPropertyValueMapping = FindMappingByInternalId(projectId, customPropertyValue.Value, customPropertyValueMappings);
                                                    if (customPropertyValueMapping != null)
                                                    {
                                                        string externalCustomFieldValue = customPropertyValueMapping.ExternalKey;

                                                        //See if we have one of the special standard TFS fields that it maps to
                                                        if (externalCustomField == TFS_SPECIAL_FIELD_AREA)
                                                        {
                                                            //Now set the value of the work item's area
                                                            int areaId = -1;
                                                            if (Int32.TryParse(externalCustomFieldValue, out areaId))
                                                            {
                                                                workItem.AreaId = areaId;
                                                            }
                                                            else
                                                            {
                                                                eventLog.WriteEntry("The area external key " + externalCustomFieldValue + " in project " + projectId + " is invalid - it needs to be numeric!", EventLogEntryType.Warning);
                                                            }
                                                        }
                                                        else if (externalCustomField == TFS_SPECIAL_FIELD_DISCIPLINE)
                                                        {
                                                            //Now set the value of the work item's triage status
                                                            workItem["Discipline"] = externalCustomFieldValue;
                                                        }
                                                        else
                                                        {
                                                            //This needs to be added to the list of TFS custom properties
                                                            workItem[externalCustomField] = externalCustomFieldValue;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

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
                                                        eventLog.WriteEntry("Error Adding " + productName + " Task to Team Foundation Server because of bad field '" + field.Name + "' (" + exception.Message + ")", EventLogEntryType.Error);
                                                    }
                                                }
                                                if (!foundInvalidField)
                                                {
                                                    //Log a general exception
                                                    eventLog.WriteEntry("Error Adding " + productName + " Task to Team Foundation Server: " + exception.Message, EventLogEntryType.Error);
                                                }
                                            }
                                            else
                                            {
                                                throw exception;
                                            }
                                        }

                                        //Now we need to update the state to the final status
                                        if (workItem.State != tfsState)
                                        {
                                            workItem.State = tfsState;
                                            workItem.Reason = "";   //The default for this state
                                            if (ValidateItem(workItem, messages))
                                            {
                                                workItem.Save();
                                            }
                                            else
                                            {
                                                //Log the detailed message as a warning because in this case we have managed to add the item already
                                                //just with the default state+reason.
                                                eventLog.WriteEntry("Warning Adding " + productName + " Task to Team Foundation Server: " + messages.ToString(), EventLogEntryType.Warning);
                                            }
                                        }

                                        //Extract the TFS Work Item ID and add to mappings table
                                        SpiraImportExport.RemoteDataMapping newTaskMapping = new SpiraImportExport.RemoteDataMapping();
                                        newTaskMapping.ProjectId = projectId;
                                        newTaskMapping.InternalId = taskId;
                                        newTaskMapping.ExternalKey = workItem.Id.ToString();
                                        newTaskMappings.Add(newTaskMapping);
                                    }
                                    else
                                    {
                                        //Log the detailed error message
                                        eventLog.WriteEntry("Error Adding " + productName + " Task to Team Foundation Server: " + messages.ToString(), EventLogEntryType.Error);
                                    }
                                }
                            }
                            catch (Exception exception)
                            {
                                //Log and continue execution
                                eventLog.WriteEntry("Error Adding " + productName + " Task to Team Foundation Server: " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                            }
                        }
                        //Finally we need to update the mapping data on the server before starting the second phase
                        //of the data-synchronization
                        //At this point we have potentially added tasks
                        spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_TASK, newTaskMappings.ToArray());
                        taskList = null;
                    }

                    //Finally we need to update the mapping data on the server before starting the second phase
                    //of the data-synchronization
                    //At this point we have potentially added releases
                    spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_RELEASE, newReleaseMappings.ToArray());

                    //Refresh the mappings from the server
                    newIncidentMappings = new List<RemoteDataMapping>();
                    newTaskMappings = new List<RemoteDataMapping>();
                    newReleaseMappings = new List<RemoteDataMapping>();
                    incidentMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_INCIDENT);
                    taskMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_TASK);
                    releaseMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_RELEASE);

                    //**** Next we need to see if any new work items were logged in TFS ****
                    DateTime startingDate = lastSyncDate.Value.AddHours(-timeOffsetHours);
                    string fieldList = "";
                    foreach (FieldDefinition field in workItemStore.FieldDefinitions)
                    {
                        if (fieldList != "")
                        {
                            fieldList += ",";
                        }
                        fieldList += "[" + field.Name + "]";
                    }
                    string wiqlQuery = "SELECT " + fieldList + " FROM WorkItems WHERE [System.CreatedDate] >= '" + startingDate.ToShortDateString() + "' AND [System.TeamProject] = '" + tfsProject + "' ORDER BY [System.CreatedDate]";
                    WorkItemCollection workItemCollection = workItemStore.Query(wiqlQuery);
                    foreach (WorkItem workItem in workItemCollection)
                    {
                        //See if we have a task or not
                        workItem.Open();
                        if (workItem.Type == project.WorkItemTypes["Task"])
                        {
                            if (String.IsNullOrEmpty(this.syncOnlyType) || this.syncOnlyType.ToLowerInvariant() == "tasks")
                            {
                                //See if we already have this task mapped
                                SpiraImportExport.RemoteDataMapping taskMapping = FindMappingByExternalKey(projectId, workItem.Id.ToString(), taskMappings, false);
                                if (taskMapping == null)
                                {
                                    //We need to add a new task to SpiraTeam
                                    RemoteTask remoteTask = new RemoteTask();
                                    remoteTask.ProjectId = projectId;

                                    //Update the task with the text fields
                                    if (!String.IsNullOrEmpty(workItem.Title))
                                    {
                                        remoteTask.Name = workItem.Title;
                                    }
                                    if (String.IsNullOrEmpty(workItem.Description))
                                    {
                                        remoteTask.Description = "Empty Description in TFS";
                                    }
                                    else
                                    {
                                        remoteTask.Description = workItem.Description;
                                    }

                                    //Debug logging - comment out for production code
                                    LogTraceEvent(eventLog, "Got the task name and description\n", EventLogEntryType.Information);

                                    //Now get the task status from the State mapping
                                    dataMapping = FindMappingByExternalKey(projectId, workItem.State, taskStatusMappings, true);
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

                                    //Update the dates and efforts
                                    if (workItem["Start Date"] != null)
                                    {
                                        remoteTask.StartDate = (DateTime)workItem["Start Date"];
                                    }
                                    if (workItem["Finish Date"] != null)
                                    {
                                        remoteTask.EndDate = (DateTime)workItem["Finish Date"];
                                    }

                                    //Debug logging - comment out for production code
                                    LogTraceEvent(eventLog, "Got the task dates\n", EventLogEntryType.Information);

                                    //Update the actual and estimated work
                                    if (workItem["Completed Work"] != null)
                                    {
                                        double completedWorkHours = (double)workItem["Completed Work"];
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
                                    foreach (SpiraImportExport.RemoteCustomProperty customProperty in taskProjectCustomProperties)
                                    {
                                        //First the text fields
                                        if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_TEXT)
                                        {
                                            if (customProperty.Alias == TFS_SPECIAL_FIELD_RANK)
                                            {
                                                //Now we need to set the value on the SpiraTest task
                                                SetCustomPropertyTextValue(remoteTask, customProperty.CustomPropertyName, (string)workItem["Rank"]);
                                            }
                                        }

                                        //Next the list fields
                                        if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_LIST)
                                        {
                                            if (customProperty.Alias == TFS_SPECIAL_FIELD_AREA)
                                            {
                                                //Now we need to set the value on the SpiraTest task
                                                SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = taskCustomPropertyValueMappingList[customProperty.CustomPropertyId];
                                                SpiraImportExport.RemoteDataMapping customPropertyValueMapping = FindMappingByExternalKey(projectId, workItem.AreaId.ToString(), customPropertyValueMappings, false);
                                                if (customPropertyValueMapping != null)
                                                {
                                                    SetCustomPropertyListValue(remoteTask, customProperty.CustomPropertyName, customPropertyValueMapping.InternalId);
                                                }
                                            }
                                            if (customProperty.Alias == TFS_SPECIAL_FIELD_DISCIPLINE && workItem["Discipline"] != null)
                                            {
                                                //Now we need to set the value on the SpiraTest task
                                                SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = taskCustomPropertyValueMappingList[customProperty.CustomPropertyId];
                                                SpiraImportExport.RemoteDataMapping customPropertyValueMapping = FindMappingByExternalKey(projectId, (string)workItem["Discipline"], customPropertyValueMappings, false);
                                                if (customPropertyValueMapping != null)
                                                {
                                                    SetCustomPropertyListValue(remoteTask, customProperty.CustomPropertyName, customPropertyValueMapping.InternalId);
                                                }
                                            }
                                        }
                                    }

                                    //Debug logging - comment out for production code
                                    LogTraceEvent(eventLog, "Got the task custom properties\n", EventLogEntryType.Information);

                                    //SpiraTest doesn't currently support resolutions in tasks, so ignoring
                                    if (String.IsNullOrEmpty((string)workItem[CoreField.AssignedTo]))
                                    {
                                        remoteTask.OwnerId = null;
                                    }
                                    else
                                    {
                                        dataMapping = FindMappingByExternalKey((string)workItem[CoreField.AssignedTo], userMappings);
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

                                    //Specify the resolved-in release if applicable
                                    if (!String.IsNullOrEmpty(workItem.IterationPath))
                                    {
                                        //See if we have a mapped SpiraTest release
                                        dataMapping = FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), releaseMappings, false);
                                        if (dataMapping == null)
                                        {
                                            //Now check to see if recently added
                                            dataMapping = FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), newReleaseMappings.ToArray(), false);
                                        }
                                        if (dataMapping == null)
                                        {
                                            //We can't find the matching item so need to create a new release in SpiraTest and add to mappings

                                            //Need to iterate through the TFS iteration node tree to get the full node object
                                            Node iterationNode = GetMatchingNode(project.IterationRootNodes, workItem.IterationId);
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
                                }
                            }
                        }
                        else
                        {
                            if (String.IsNullOrEmpty(this.syncOnlyType) || this.syncOnlyType.ToLowerInvariant() == "incidents")
                            {
                                //See if we already have this incident mapped
                                SpiraImportExport.RemoteDataMapping incidentMapping = FindMappingByExternalKey(projectId, workItem.Id.ToString(), incidentMappings, false);
                                if (incidentMapping == null)
                                {
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
                                    if (String.IsNullOrEmpty(workItem.Description))
                                    {
                                        remoteIncident.Description = "Empty Description in TFS";
                                    }
                                    else
                                    {
                                        remoteIncident.Description = workItem.Description;
                                    }

                                    //Debug logging - comment out for production code
                                    LogTraceEvent(eventLog, "Got the incident name and description\n", EventLogEntryType.Information);

                                    //Get the type of the incident
                                    dataMapping = FindMappingByExternalKey(projectId, workItem.Type.Name, incidentTypeMappings, true);
                                    if (dataMapping == null)
                                    {
                                        //We can't find the matching item so log and just don't set the priority
                                        eventLog.WriteEntry("Unable to locate mapping entry for work item type " + workItem.Type.Name + " in project " + projectId, EventLogEntryType.Error);
                                        continue;
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
                                            dataMapping = FindMappingByExternalKey(projectId, workItem["Priority"].ToString(), incidentPriorityMappings, true);
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
                                    dataMapping = FindMappingByExternalKey(projectId, stateAndReason, incidentStatusMappings, true);
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
                                    foreach (SpiraImportExport.RemoteCustomProperty customProperty in incidentProjectCustomProperties)
                                    {
                                        //First the text fields
                                        if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_TEXT)
                                        {
                                            if (customProperty.Alias == TFS_SPECIAL_FIELD_RANK)
                                            {
                                                //Now we need to set the value on the SpiraTest incident
                                                SetCustomPropertyTextValue(remoteIncident, customProperty.CustomPropertyName, (string)workItem["Rank"]);
                                            }
                                        }

                                        //Next the list fields
                                        if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_LIST)
                                        {
                                            if (customProperty.Alias == TFS_SPECIAL_FIELD_AREA)
                                            {
                                                //Now we need to set the value on the SpiraTest incident
                                                SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = incidentCustomPropertyValueMappingList[customProperty.CustomPropertyId];
                                                SpiraImportExport.RemoteDataMapping customPropertyValueMapping = FindMappingByExternalKey(projectId, workItem.AreaId.ToString(), customPropertyValueMappings, false);
                                                if (customPropertyValueMapping != null)
                                                {
                                                    SetCustomPropertyListValue(remoteIncident, customProperty.CustomPropertyName, customPropertyValueMapping.InternalId);
                                                }
                                            }
                                            if (customProperty.Alias == TFS_SPECIAL_FIELD_TRIAGE)
                                            {
                                                //Now we need to set the value on the SpiraTest incident
                                                SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = incidentCustomPropertyValueMappingList[customProperty.CustomPropertyId];
                                                SpiraImportExport.RemoteDataMapping customPropertyValueMapping = FindMappingByExternalKey(projectId, (string)workItem["Triage"], customPropertyValueMappings, false);
                                                if (customPropertyValueMapping != null)
                                                {
                                                    SetCustomPropertyListValue(remoteIncident, customProperty.CustomPropertyName, customPropertyValueMapping.InternalId);
                                                }
                                            }
                                        }
                                    }

                                    if (String.IsNullOrEmpty((string)workItem[CoreField.AssignedTo]))
                                    {
                                        remoteIncident.OwnerId = null;
                                    }
                                    else
                                    {
                                        dataMapping = FindMappingByExternalKey((string)workItem[CoreField.AssignedTo], userMappings);
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
                                        dataMapping = FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), releaseMappings, false);
                                        if (dataMapping == null)
                                        {
                                            //Now check to see if recently added
                                            dataMapping = FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), newReleaseMappings.ToArray(), false);
                                        }
                                        if (dataMapping == null)
                                        {
                                            //We can't find the matching item so need to create a new release in SpiraTest and add to mappings

                                            //Need to iterate through the TFS iteration node tree to get the full node object
                                            Node iterationNode = GetMatchingNode(project.IterationRootNodes, workItem.IterationId);
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
                                    List<SpiraImportExport.RemoteIncidentResolution> newIncidentResolutions = new List<SpiraImportExport.RemoteIncidentResolution>();
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
                                                int creatorId = -1;
                                                dataMapping = FindMappingByExternalKey(revisionCreatedBy, userMappings);
                                                if (dataMapping == null)
                                                {
                                                    //Finally we just fallback to using the synchronization user (i.e. the reporter in TFS)
                                                    dataMapping = FindMappingByExternalKey(workItem.CreatedBy, userMappings);
                                                    if (dataMapping != null)
                                                    {
                                                        creatorId = dataMapping.InternalId;
                                                    }
                                                }
                                                else
                                                {
                                                    creatorId = dataMapping.InternalId;
                                                }
                                                if (creatorId == -1)
                                                {
                                                    //We can't find the matching item so log and ignore
                                                    eventLog.WriteEntry("Unable to locate mapping entry for user " + revisionCreatedBy, EventLogEntryType.Error);
                                                }
                                                else
                                                {
                                                    LogTraceEvent(eventLog, "Got the resolution creator: " + creatorId.ToString() + "\n", EventLogEntryType.Information);

                                                    //Add the comment to SpiraTest
                                                    SpiraImportExport.RemoteIncidentResolution newIncidentResolution = new SpiraImportExport.RemoteIncidentResolution();
                                                    newIncidentResolution.IncidentId = incidentId;
                                                    newIncidentResolution.CreatorId = creatorId;
                                                    newIncidentResolution.CreationDate = (DateTime)revision.Fields[CoreField.ChangedDate].Value;
                                                    newIncidentResolution.Resolution = (string)revision.Fields[CoreField.History].Value;
                                                    newIncidentResolutions.Add(newIncidentResolution);
                                                }
                                            }
                                        }
                                    }
                                    spiraImportExport.Incident_AddResolutions(newIncidentResolutions.ToArray());

                                    //Debug logging - comment out for production code
                                    LogTraceEvent(eventLog, "Got the comments/history\n", EventLogEntryType.Information);
                                }
                            }
                        }
                    }
                    //Save any new mappings
                    spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_INCIDENT, newIncidentMappings.ToArray());
                    spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_TASK, newTaskMappings.ToArray());
                    spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_RELEASE, newReleaseMappings.ToArray());

                    //Refresh the mappings from the server
                    if (String.IsNullOrEmpty(this.syncOnlyType) || this.syncOnlyType.ToLowerInvariant() == "incidents")
                    {
                        incidentMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_INCIDENT);
                    }
                    else
                    {
                        //We're not syncing incidents to just create a zero-entry array
                        incidentMappings = new RemoteDataMapping[0];
                    }
                    if (String.IsNullOrEmpty(this.syncOnlyType) || this.syncOnlyType.ToLowerInvariant() == "tasks")
                    {
                        taskMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_TASK);
                    }
                    else
                    {
                        //We're not syncing tasks to just create a zero-entry array
                        taskMappings = new RemoteDataMapping[0];
                    }
                    releaseMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_RELEASE);

                    //**** Next we need to see if any of the previously mapped incidents have changed in either system ****

                    //Need to create a list to hold any new releases
                    newReleaseMappings = new List<RemoteDataMapping>();

                    //Iterate through each of the mapped items
                    foreach (SpiraImportExport.RemoteDataMapping incidentMapping in incidentMappings)
                    {
                        //Get the SpiraTest and TFS incident/work item ids
                        int incidentId = incidentMapping.InternalId;
                        int workItemId = Int32.Parse(incidentMapping.ExternalKey);
                        
                        //Make sure it's for the current project
                        if (incidentMapping.ProjectId == projectId)
                        {
                            //Now retrieve the SpiraTest incident using the Import APIs
                            SpiraImportExport.RemoteIncident remoteIncident = spiraImportExport.Incident_RetrieveById(incidentId);

                            //Now retrieve the work item from MSTFS
                            WorkItem workItem = null;
                            try
                            {
                                workItem = workItemStore.GetWorkItem(workItemId);
                            }
                            catch (Exception)
                            {
                                //Handle exceptions quietly since work item may have been deleted
                            }

                            //Make sure we have retrieved the work item (may have been deleted)
                            if (remoteIncident != null && workItem != null)
                            {
                                try
                                {
                                    //Now check to see if we have a change in TFS or SpiraTeam since we last ran
                                    //Only apply the timeoffset to TFS as the data-sync runs on the same server as SpiraTeam
                                    string updateMode = "";
                                    if ((workItem.ChangedDate.AddHours(timeOffsetHours).AddMinutes(5)) > lastSyncDate)
                                    {
                                        updateMode = "TFS=Newer";
                                    }
                                    if (remoteIncident.LastUpdateDate > lastSyncDate)
                                    {
                                        if (updateMode == "")
                                        {
                                            updateMode = "Spira=Newer";
                                        }
                                        else
                                        {
                                            if (workItem.ChangedDate.AddHours(timeOffsetHours) > remoteIncident.LastUpdateDate)
                                            {
                                                updateMode = "TFS=Newer";
                                            }
                                            else
                                            {
                                                updateMode = "Spira=Newer";
                                            }
                                        }
                                    }
                                    if (updateMode != "")
                                    {
                                        LogTraceEvent(eventLog, "Update Mode is " + updateMode + "\n", EventLogEntryType.SuccessAudit);
                                    }

                                    //Handle the case where we need to move data SpiraTeam > TFS
                                    if (updateMode == "Spira=Newer")
                                    {
                                        //We need to track if any changes were made and only update in that case
                                        //to avoid the issue of perpetual updates
                                        bool changesMade = false;

                                        //Get certain incident fields into local variables (if used more than once)
                                        int incidentStatusId = remoteIncident.IncidentStatusId;

                                        //Now get the work item type from the mapping
                                        //Tasks are handled separately unless they are mapped, need to check
                                        dataMapping = FindMappingByInternalId(projectId, remoteIncident.IncidentTypeId, incidentTypeMappings);
                                        if (dataMapping == null)
                                        {
                                            //We can't find the matching item so log and move to the next incident
                                            eventLog.WriteEntry("Unable to locate mapping entry for incident type " + remoteIncident.IncidentTypeId + " in project " + projectId, EventLogEntryType.Error);
                                            continue;
                                        }
                                        string workItemTypeName = dataMapping.ExternalKey;

                                        //First we need to get the Iteration, mapped from the SpiraTest Release, if not create it
                                        //Need to do this before creating the work item as we may need to reload the project reference
                                        int iterationId = -1;
                                        if (remoteIncident.ResolvedReleaseId.HasValue)
                                        {
                                            int detectedReleaseId = remoteIncident.ResolvedReleaseId.Value;
                                            dataMapping = FindMappingByInternalId(projectId, detectedReleaseId, releaseMappings);
                                            if (dataMapping == null)
                                            {
                                                //Now check to see if recently added
                                                dataMapping = FindMappingByInternalId(projectId, detectedReleaseId, newReleaseMappings.ToArray());
                                            }
                                            if (dataMapping == null)
                                            {
                                                //We can't find the matching item so need to create a new iteration in TFS and add to mappings
                                                LogTraceEvent(eventLog, "Adding new iteration in TFS for release " + detectedReleaseId + "\n", EventLogEntryType.Information);
                                                Node newIterationNode = AddNewTfsIteration(teamFoundationServer, ref workItemStore, ref project, remoteIncident.ResolvedReleaseVersionNumber);

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
                                            dataMapping = FindMappingByInternalId(projectId, detectedReleaseId, releaseMappings);
                                            if (dataMapping == null)
                                            {
                                                //Now check to see if recently added
                                                dataMapping = FindMappingByInternalId(projectId, detectedReleaseId, newReleaseMappings.ToArray());
                                            }
                                            if (dataMapping == null)
                                            {
                                                //We can't find the matching item so need to create a new iteration in TFS and add to mappings
                                                LogTraceEvent(eventLog, "Adding new iteration in TFS for release " + detectedReleaseId + "\n", EventLogEntryType.Information);
                                                Node newIterationNode = AddNewTfsIteration(teamFoundationServer, ref workItemStore, ref project, remoteIncident.DetectedReleaseVersionNumber);

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
                                        string description = HtmlRenderAsPlainText(remoteIncident.Description);
                                        if (workItem.Description != description)
                                        {
                                            workItem.Description = description;
                                            changesMade = true;
                                        }
                                        if (iterationId != -1 && workItem.IterationId != iterationId)
                                        {
                                            changesMade = true;
                                            workItem.IterationId = iterationId;
                                        }

                                        //Now get the incident status from the mapping
                                        dataMapping = FindMappingByInternalId(projectId, remoteIncident.IncidentStatusId, incidentStatusMappings);
                                        if (dataMapping == null)
                                        {
                                            //We can't find the matching item so log and move to the next incident
                                            eventLog.WriteEntry("Unable to locate mapping entry for incident status " + remoteIncident.IncidentStatusId + " in project " + projectId, EventLogEntryType.Error);
                                            continue;
                                        }
                                        //The status in SpiraTest = MSTFS State+Reason
                                        string[] stateAndReason = dataMapping.ExternalKey.Split('+');
                                        string tfsState = stateAndReason[0];
                                        string tfsReason = stateAndReason[1];

                                        //Now get the incident priority from the mapping (if priority is set)
                                        try
                                        {
                                            if (remoteIncident.PriorityId.HasValue)
                                            {
                                                dataMapping = FindMappingByInternalId(projectId, remoteIncident.PriorityId.Value, incidentPriorityMappings);
                                                if (dataMapping == null)
                                                {
                                                    //We can't find the matching item so log and just don't set the priority
                                                    eventLog.WriteEntry("Unable to locate mapping entry for incident priority " + remoteIncident.PriorityId.Value + " in project " + projectId, EventLogEntryType.Warning);
                                                }
                                                else
                                                {
                                                    if (workItem.Fields.Contains("Priority") && workItem.Fields["Priority"].IsValid)
                                                    {
                                                        if (workItem["Priority"].ToString() != dataMapping.ExternalKey)
                                                        {
                                                            workItem["Priority"] = dataMapping.ExternalKey;
                                                            changesMade = true;
                                                        }
                                                    }
                                                }
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

                                        //The creator is not allowed to be set on the work-item (read-only)

                                        //Now set the assignee
                                        if (remoteIncident.OwnerId.HasValue)
                                        {
                                            dataMapping = FindMappingByInternalId(remoteIncident.OwnerId.Value, userMappings);
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

                                        //Now iterate through the project custom properties
                                        foreach (SpiraImportExport.RemoteCustomProperty customProperty in incidentProjectCustomProperties)
                                        {
                                            //Handle list and text ones separately
                                            if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_TEXT)
                                            {
                                                //See if we have a custom property value set
                                                String customPropertyValue = GetCustomPropertyTextValue(remoteIncident, customProperty.CustomPropertyName);
                                                if (!String.IsNullOrEmpty(customPropertyValue))
                                                {
                                                    //Get the corresponding external custom field (if there is one)
                                                    if (incidentCustomPropertyMappingList.ContainsKey(customProperty.CustomPropertyId))
                                                    {
                                                        string externalCustomField = incidentCustomPropertyMappingList[customProperty.CustomPropertyId].ExternalKey;

                                                        //See if we have one of the special standard TFS field that it maps to
                                                        if (externalCustomField == TFS_SPECIAL_FIELD_RANK)
                                                        {
                                                            if (workItem["Rank"].ToString() != customPropertyValue)
                                                            {
                                                                workItem["Rank"] = customPropertyValue;
                                                                changesMade = true;
                                                            }
                                                        }
                                                        else
                                                        {
                                                            //This needs to be added to the list of TFS custom properties
                                                            if (workItem[externalCustomField].ToString() != customPropertyValue)
                                                            {
                                                                workItem[externalCustomField] = customPropertyValue;
                                                                changesMade = true;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_LIST)
                                            {
                                                //See if we have a custom property value set
                                                Nullable<int> customPropertyValue = GetCustomPropertyListValue(remoteIncident, customProperty.CustomPropertyName);

                                                //Get the corresponding external custom field (if there is one)
                                                if (customPropertyValue.HasValue && incidentCustomPropertyMappingList.ContainsKey(customProperty.CustomPropertyId))
                                                {
                                                    string externalCustomField = incidentCustomPropertyMappingList[customProperty.CustomPropertyId].ExternalKey;

                                                    //Get the corresponding external custom field value (if there is one)
                                                    if (incidentCustomPropertyValueMappingList.ContainsKey(customProperty.CustomPropertyId))
                                                    {
                                                        SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = incidentCustomPropertyValueMappingList[customProperty.CustomPropertyId];
                                                        SpiraImportExport.RemoteDataMapping customPropertyValueMapping = FindMappingByInternalId(projectId, customPropertyValue.Value, customPropertyValueMappings);
                                                        if (customPropertyValueMapping != null)
                                                        {
                                                            string externalCustomFieldValue = customPropertyValueMapping.ExternalKey;

                                                            //See if we have one of the special standard TFS fields that it maps to
                                                            if (externalCustomField == TFS_SPECIAL_FIELD_AREA)
                                                            {
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
                                                            else if (externalCustomField == TFS_SPECIAL_FIELD_TRIAGE)
                                                            {
                                                                //Now set the value of the work item's triage status
                                                                if (workItem.Fields.Contains("Triage"))
                                                                {
                                                                    if (workItem["Triage"].ToString() != externalCustomFieldValue)
                                                                    {
                                                                        workItem["Triage"] = externalCustomFieldValue;
                                                                        changesMade = true;
                                                                    }
                                                                }
                                                            }
                                                            else
                                                            {
                                                                //This needs to be added to the list of TFS custom properties
                                                                if (workItem[externalCustomField].ToString() != externalCustomFieldValue)
                                                                {
                                                                    workItem[externalCustomField] = externalCustomFieldValue;
                                                                    changesMade = true;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }

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

                                        //Finally add any new resolutions to the history
                                        RemoteIncidentResolution[] remoteResolutions = spiraImportExport.Incident_RetrieveResolutions(incidentId);
                                        foreach (RemoteIncidentResolution remoteResolution in remoteResolutions)
                                        {
                                            foreach (Revision revision in workItem.Revisions)
                                            {
                                                //Add the author, date and body to the resolution
                                                if (revision.Fields[CoreField.History].Value != null && revision.Fields[CoreField.History].Value.ToString() != "")
                                                {
                                                    //See if we have one that's not already there (and make sure the comment is more recent than changes in TFS)
                                                    string resolutionDescription = HtmlRenderAsPlainText(remoteResolution.Resolution);
                                                    if ((string)revision.Fields[CoreField.History].Value != resolutionDescription && remoteResolution.CreationDate > (workItem.ChangedDate.AddHours(timeOffsetHours).AddMinutes(5)))
                                                    {
                                                        workItem.History = resolutionDescription;
                                                        changesMade = true;
                                                        break;
                                                    }
                                                }
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
                                        //We need to track if any changes were made and only update in that case
                                        //to avoid the issue of perpetual updates
                                        bool changesMade = false;
                                        
                                        //Update the incident with the text fields
                                        if (!String.IsNullOrEmpty(workItem.Title) && remoteIncident.Name != workItem.Title)
                                        {
                                            remoteIncident.Name = workItem.Title;
                                            changesMade = true;
                                        }
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

                                        //Debug logging - comment out for production code
                                        LogTraceEvent(eventLog, "Got the incident name and description\n", EventLogEntryType.Information);

                                        //Now get the work item priority from the mapping (if priority is set)
                                        try
                                        {
                                            if (workItem.Fields.Contains("Priority") && workItem.Fields["Priority"].IsValid)
                                            {
                                                if (String.IsNullOrEmpty(workItem["Priority"].ToString()))
                                                {
                                                    if (remoteIncident.PriorityId.HasValue)
                                                    {
                                                        remoteIncident.PriorityId = null;
                                                        changesMade = true;
                                                    }
                                                }
                                                else
                                                {
                                                    dataMapping = FindMappingByExternalKey(projectId, workItem["Priority"].ToString(), incidentPriorityMappings, true);
                                                    if (dataMapping == null)
                                                    {
                                                        //We can't find the matching item so log and just don't set the priority
                                                        eventLog.WriteEntry("Unable to locate mapping entry for work item priority " + workItem["Priority"].ToString() + " in project " + projectId, EventLogEntryType.Warning);
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

                                        //Now get the work item status from the State+Reason mapping
                                        string stateAndReason = workItem.State + "+" + workItem.Reason;
                                        dataMapping = FindMappingByExternalKey(projectId, stateAndReason, incidentStatusMappings, true);
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
                                        foreach (SpiraImportExport.RemoteCustomProperty customProperty in incidentProjectCustomProperties)
                                        {
                                            //First the text fields
                                            if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_TEXT)
                                            {
                                                if (customProperty.Alias == TFS_SPECIAL_FIELD_RANK)
                                                {
                                                    //Now we need to set the value on the SpiraTest incident
                                                    changesMade = SetCustomPropertyTextValue(remoteIncident, customProperty.CustomPropertyName, (string)workItem["Rank"], changesMade);
                                                }
                                            }

                                            //Next the list fields
                                            if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_LIST)
                                            {
                                                if (customProperty.Alias == TFS_SPECIAL_FIELD_AREA)
                                                {
                                                    //Now we need to set the value on the SpiraTest incident
                                                    SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = incidentCustomPropertyValueMappingList[customProperty.CustomPropertyId];
                                                    SpiraImportExport.RemoteDataMapping customPropertyValueMapping = FindMappingByExternalKey(projectId, workItem.AreaId.ToString(), customPropertyValueMappings, false);
                                                    if (customPropertyValueMapping != null)
                                                    {
                                                        changesMade = SetCustomPropertyListValue(remoteIncident, customProperty.CustomPropertyName, customPropertyValueMapping.InternalId, changesMade);
                                                    }
                                                }
                                                if (customProperty.Alias == TFS_SPECIAL_FIELD_TRIAGE)
                                                {
                                                    //Now we need to set the value on the SpiraTest incident
                                                    if (workItem.Fields.Contains("Triage"))
                                                    {
                                                        SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = incidentCustomPropertyValueMappingList[customProperty.CustomPropertyId];
                                                        SpiraImportExport.RemoteDataMapping customPropertyValueMapping = FindMappingByExternalKey(projectId, (string)workItem["Triage"], customPropertyValueMappings, false);
                                                        if (customPropertyValueMapping != null)
                                                        {
                                                            changesMade = SetCustomPropertyListValue(remoteIncident, customProperty.CustomPropertyName, customPropertyValueMapping.InternalId, changesMade);
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        //Now we need to get all the comments attached to the work item in TFS
                                        RevisionCollection revisions = workItem.Revisions;

                                        //Now get the list of comments attached to the SpiraTest incident
                                        SpiraImportExport.RemoteIncidentResolution[] incidentResolutions = spiraImportExport.Incident_RetrieveResolutions(incidentId);

                                        //Iterate through all the comments and see if we need to add any to SpiraTest
                                        List<SpiraImportExport.RemoteIncidentResolution> newIncidentResolutions = new List<SpiraImportExport.RemoteIncidentResolution>();
                                        if (revisions != null)
                                        {
                                            foreach (Revision revision in revisions)
                                            {
                                                //Add the author, date and body to the resolution
                                                if (revision.Fields[CoreField.History].Value != null && revision.Fields[CoreField.History].Value.ToString() != "")
                                                {
                                                    //See if we already have this resolution inside SpiraTest
                                                    bool alreadyAdded = false;
                                                    foreach (SpiraImportExport.RemoteIncidentResolution incidentResolution in incidentResolutions)
                                                    {
                                                        if (incidentResolution.Resolution == (string)revision.Fields[CoreField.History].Value)
                                                        {
                                                            alreadyAdded = true;
                                                        }
                                                    }
                                                    if (!alreadyAdded)
                                                    {
                                                        //Get the resolution author mapping
                                                        string revisionCreatedBy = (string)revision.Fields[CoreField.ChangedBy].Value;
                                                        LogTraceEvent(eventLog, "Looking for comments author: '" + revisionCreatedBy + "'\n", EventLogEntryType.Information);
                                                        int creatorId = -1;
                                                        dataMapping = FindMappingByExternalKey(revisionCreatedBy, userMappings);
                                                        if (dataMapping == null)
                                                        {
                                                            //Finally we just fallback to using the synchronization user (i.e. the reporter in TFS)
                                                            dataMapping = FindMappingByExternalKey(workItem.CreatedBy, userMappings);
                                                            if (dataMapping != null)
                                                            {
                                                                creatorId = dataMapping.InternalId;
                                                            }
                                                        }
                                                        else
                                                        {
                                                            creatorId = dataMapping.InternalId;
                                                        }
                                                        if (creatorId == -1)
                                                        {
                                                            //We can't find the matching item so log and ignore
                                                            eventLog.WriteEntry("Unable to locate mapping entry for user " + revisionCreatedBy, EventLogEntryType.Error);
                                                        }
                                                        else
                                                        {
                                                            LogTraceEvent(eventLog, "Got the resolution creator: " + creatorId.ToString() + "\n", EventLogEntryType.Information);

                                                            //Add the comment to SpiraTest
                                                            SpiraImportExport.RemoteIncidentResolution newIncidentResolution = new SpiraImportExport.RemoteIncidentResolution();
                                                            newIncidentResolution.IncidentId = incidentId;
                                                            newIncidentResolution.CreatorId = creatorId;
                                                            newIncidentResolution.CreationDate = (DateTime)revision.Fields[CoreField.ChangedDate].Value;
                                                            newIncidentResolution.Resolution = (string)revision.Fields[CoreField.History].Value;
                                                            newIncidentResolutions.Add(newIncidentResolution);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        spiraImportExport.Incident_AddResolutions(newIncidentResolutions.ToArray());

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
                                            dataMapping = FindMappingByExternalKey((string)workItem[CoreField.AssignedTo], userMappings);
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
                                            dataMapping = FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), releaseMappings, false);
                                            if (dataMapping == null)
                                            {
                                                //Now check to see if recently added
                                                dataMapping = FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), newReleaseMappings.ToArray(), false);
                                            }
                                            if (dataMapping == null)
                                            {
                                                //We can't find the matching item so need to create a new release in SpiraTest and add to mappings

                                                //Need to iterate through the TFS iteration node tree to get the full node object
                                                Node iterationNode = GetMatchingNode(project.IterationRootNodes, workItem.IterationId);
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
                                    }
                                }
                                catch (Exception exception)
                                {
                                    //Log and continue execution
                                    eventLog.WriteEntry("Error Synchronizing Incidents between TFS and " + productName + ": " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                                }
                            }
                        }
                    }

                    //**** Next we need to see if any of the previously mapped tasks has changed in either system ****

                    //Iterate through each of the mapped items
                    foreach (SpiraImportExport.RemoteDataMapping taskMapping in taskMappings)
                    {
                        //Get the SpiraTest and TFS task/work item ids
                        int taskId = taskMapping.InternalId;
                        int workItemId = Int32.Parse(taskMapping.ExternalKey);

                        //Make sure it's for the current project
                        if (taskMapping.ProjectId == projectId)
                        {
                            //Now retrieve the SpiraTest task using the Import APIs
                            SpiraImportExport.RemoteTask remoteTask = spiraImportExport.Task_RetrieveById(taskId);

                            //Now retrieve the work item from MSTFS
                            WorkItem workItem = null;
                            try
                            {
                                workItem = workItemStore.GetWorkItem(workItemId);
                            }
                            catch (Exception)
                            {
                                //Handle exceptions quietly since work item may have been deleted
                            }

                            //Make sure we have retrieved the work item (may have been deleted)
                            if (remoteTask != null && workItem != null)
                            {
                                try
                                {
                                    //Now check to see if we have a change in TFS or SpiraTeam since we last ran
                                    //Only apply the timeoffset to TFS as the data-sync runs on the same server as SpiraTeam
                                    string updateMode = "";
                                    if ((workItem.ChangedDate.AddHours(timeOffsetHours).AddMinutes(5)) > lastSyncDate)
                                    {
                                        updateMode = "TFS=Newer";
                                    }
                                    if (remoteTask.LastUpdateDate > lastSyncDate)
                                    {
                                        if (updateMode == "")
                                        {
                                            updateMode = "Spira=Newer";
                                        }
                                        else
                                        {
                                            if (workItem.ChangedDate.AddHours(timeOffsetHours) > remoteTask.LastUpdateDate)
                                            {
                                                updateMode = "TFS=Newer";
                                            }
                                            else
                                            {
                                                updateMode = "Spira=Newer";
                                            }
                                        }
                                    }
                                    if (updateMode != "")
                                    {
                                        LogTraceEvent(eventLog, "Update Mode is " + updateMode + "\n", EventLogEntryType.SuccessAudit);
                                    }

                                    //Handle the case where we need to move data SpiraTeam > TFS
                                    if (updateMode == "Spira=Newer")
                                    {
                                        //We need to track if any changes were made and only update in that case
                                        //to avoid the issue of perpetual updates
                                        bool changesMade = false;

                                        //Get certain task fields into local variables (if used more than once)
                                        int taskStatusId = remoteTask.TaskStatusId;

                                        //Note: Tasks always map to the Task Work Item Type in TFS

                                        //First we need to get the Iteration, mapped from the SpiraTest Release, if not create it
                                        //Need to do this before creating the work item as we may need to reload the project reference
                                        int iterationId = -1;
                                        if (remoteTask.ReleaseId.HasValue)
                                        {
                                            int detectedReleaseId = remoteTask.ReleaseId.Value;
                                            dataMapping = FindMappingByInternalId(projectId, detectedReleaseId, releaseMappings);
                                            if (dataMapping == null)
                                            {
                                                //We can't find the matching item so need to create a new iteration in TFS and add to mappings
                                                LogTraceEvent(eventLog, "Adding new iteration in TFS for release " + detectedReleaseId + "\n", EventLogEntryType.Information);
                                                Node newIterationNode = AddNewTfsIteration(teamFoundationServer, ref workItemStore, ref project, remoteTask.ReleaseVersionNumber);

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

                                        //Now, update the TFS work item and populate the standard fields that don't need mapping
                                        if (workItem.Title != remoteTask.Name)
                                        {
                                            workItem.Title = remoteTask.Name;
                                            changesMade = true;
                                        }
                                        if (!String.IsNullOrEmpty(remoteTask.Description))
                                        {
                                            string description = HtmlRenderAsPlainText(remoteTask.Description);
                                            if (workItem.Description != description)
                                            {
                                                workItem.Description = description;
                                                changesMade = true;
                                            }
                                        }
                                        LogTraceEvent(eventLog, "Updated title and description\n", EventLogEntryType.Information);

                                        if (iterationId != -1 && workItem.IterationId != iterationId)
                                        {
                                            changesMade = true;
                                            workItem.IterationId = iterationId;
                                        }

                                        try
                                        {
                                            if (remoteTask.StartDate.HasValue)
                                            {
                                                if (workItem.Fields.Contains("Start Date"))
                                                {
                                                    if (workItem["Start Date"].ToString() != remoteTask.StartDate.ToString())
                                                    {
                                                        workItem["Start Date"] = remoteTask.StartDate.Value;
                                                        changesMade = true;
                                                    }
                                                }
                                            }
                                            if (remoteTask.EndDate.HasValue)
                                            {
                                                if (workItem.Fields.Contains("Finish Date"))
                                                {
                                                    if (workItem["Finish Date"].ToString() != remoteTask.EndDate.ToString())
                                                    {
                                                        workItem["Finish Date"] = remoteTask.EndDate.Value;
                                                        changesMade = true;
                                                    }
                                                }
                                            }
                                            LogTraceEvent(eventLog, "Updated dates\n", EventLogEntryType.Information);
                                        }
                                        catch (Exception)
                                        {
                                            //Ignore errors as some installations won't let the dates be set
                                        }

                                        //The Remaining Work and Completed Work fields should only flow
                                        //from TFS > Spira to avoid race conditions since the methodologies
                                        //are not exactly the same and developers will be working in TFS

                                        //Now get the task status from the mapping
                                        dataMapping = FindMappingByInternalId(projectId, remoteTask.TaskStatusId, taskStatusMappings);
                                        if (dataMapping == null)
                                        {
                                            //We can't find the matching item so log and move to the next task
                                            eventLog.WriteEntry("Unable to locate mapping entry for task status " + remoteTask.TaskStatusId + " in project " + projectId, EventLogEntryType.Error);
                                            continue;
                                        }
                                        //The status in SpiraTest = MSTFS State only for tasks
                                        string tfsState = dataMapping.ExternalKey;

                                        //The creator is not allowed to be set on the work-item (read-only)

                                        //Now set the assignee
                                        if (remoteTask.OwnerId.HasValue)
                                        {
                                            dataMapping = FindMappingByInternalId(remoteTask.OwnerId.Value, userMappings);
                                            if (dataMapping == null)
                                            {
                                                //We can't find the matching user so ignore
                                                eventLog.WriteEntry("Unable to locate mapping entry for user id " + remoteTask.OwnerId.Value + " so leaving blank", EventLogEntryType.Warning);
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
                                        LogTraceEvent(eventLog, "Updated owner\n", EventLogEntryType.Information);

                                        //Now iterate through the project custom properties
                                        if (taskProjectCustomProperties != null)
                                        {
                                            foreach (SpiraImportExport.RemoteCustomProperty customProperty in taskProjectCustomProperties)
                                            {
                                                //Handle list and text ones separately
                                                if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_TEXT)
                                                {
                                                    //See if we have a custom property value set
                                                    String customPropertyValue = GetCustomPropertyTextValue(remoteTask, customProperty.CustomPropertyName);
                                                    if (!String.IsNullOrEmpty(customPropertyValue))
                                                    {
                                                        //Get the corresponding external custom field (if there is one)
                                                        if (taskCustomPropertyMappingList.ContainsKey(customProperty.CustomPropertyId))
                                                        {
                                                            string externalCustomField = taskCustomPropertyMappingList[customProperty.CustomPropertyId].ExternalKey;

                                                            //See if we have one of the special standard TFS field that it maps to
                                                            if (externalCustomField == TFS_SPECIAL_FIELD_RANK)
                                                            {
                                                                if (workItem.Fields.Contains("Rank"))
                                                                {
                                                                    if (workItem["Rank"].ToString() != customPropertyValue)
                                                                    {
                                                                        workItem["Rank"] = customPropertyValue;
                                                                        changesMade = true;
                                                                    }
                                                                }
                                                            }
                                                            else
                                                            {
                                                                if (workItem.Fields.Contains(externalCustomField))
                                                                {
                                                                    //This needs to be added to the list of TFS custom properties
                                                                    if (workItem[externalCustomField].ToString() != customPropertyValue)
                                                                    {
                                                                        workItem[externalCustomField] = customPropertyValue;
                                                                        changesMade = true;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_LIST)
                                                {
                                                    //See if we have a custom property value set
                                                    Nullable<int> customPropertyValue = GetCustomPropertyListValue(remoteTask, customProperty.CustomPropertyName);

                                                    //Get the corresponding external custom field (if there is one)
                                                    if (customPropertyValue.HasValue && taskCustomPropertyMappingList.ContainsKey(customProperty.CustomPropertyId))
                                                    {
                                                        string externalCustomField = taskCustomPropertyMappingList[customProperty.CustomPropertyId].ExternalKey;

                                                        //Get the corresponding external custom field value (if there is one)
                                                        if (taskCustomPropertyValueMappingList.ContainsKey(customProperty.CustomPropertyId))
                                                        {
                                                            SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = taskCustomPropertyValueMappingList[customProperty.CustomPropertyId];
                                                            SpiraImportExport.RemoteDataMapping customPropertyValueMapping = FindMappingByInternalId(projectId, customPropertyValue.Value, customPropertyValueMappings);
                                                            if (customPropertyValueMapping != null)
                                                            {
                                                                string externalCustomFieldValue = customPropertyValueMapping.ExternalKey;

                                                                //See if we have one of the special standard TFS fields that it maps to
                                                                if (externalCustomField == TFS_SPECIAL_FIELD_AREA)
                                                                {
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
                                                                else if (externalCustomField == TFS_SPECIAL_FIELD_DISCIPLINE)
                                                                {
                                                                    //Now set the value of the work item's triage status
                                                                    if (workItem.Fields.Contains("Discipline"))
                                                                    {
                                                                        if (workItem["Discipline"].ToString() != externalCustomFieldValue)
                                                                        {
                                                                            workItem["Discipline"] = externalCustomFieldValue;
                                                                            changesMade = true;
                                                                        }
                                                                    }
                                                                }
                                                                else
                                                                {
                                                                    if (workItem.Fields.Contains(externalCustomField))
                                                                    {
                                                                        //This needs to be added to the list of TFS custom properties
                                                                        if (workItem[externalCustomField].ToString() != externalCustomFieldValue)
                                                                        {
                                                                            workItem[externalCustomField] = externalCustomFieldValue;
                                                                            changesMade = true;
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            LogTraceEvent(eventLog, "Updated custom properties\n", EventLogEntryType.Information);
                                        }

                                        //Set the state of the work item
                                        if (workItem.State != tfsState)
                                        {
                                            workItem.State = tfsState;
                                            changesMade = true;
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
                                                                eventLog.WriteEntry("Error Updating " + productName + " Task in Team Foundation Server because of bad field '" + field.Name + "' (" + exception.Message + ")", EventLogEntryType.Error);
                                                            }
                                                        }
                                                        if (!foundInvalidField)
                                                        {
                                                            //Log a general exception
                                                            eventLog.WriteEntry("Error Updating " + productName + " Task in Team Foundation Server: " + exception.Message, EventLogEntryType.Error);
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
                                                eventLog.WriteEntry("Error Updating " + productName + " Task in Team Foundation Server: " + messages.ToString(), EventLogEntryType.Error);
                                            }
                                        }
                                    }

                                    //Handle the case where we need to move data TFS > SpiraTeam
                                    if (updateMode == "TFS=Newer")
                                    {
                                        //We need to track if any changes were made and only update in that case
                                        //to avoid the issue of perpetual updates
                                        bool changesMade = false;

                                        //Update the task with the text fields
                                        if (!String.IsNullOrEmpty(workItem.Title) && remoteTask.Name != workItem.Title)
                                        {
                                            remoteTask.Name = workItem.Title;
                                            changesMade = true;
                                        }
                                        if (String.IsNullOrEmpty(workItem.Description))
                                        {
                                            remoteTask.Description = "Empty Description in TFS";
                                        }
                                        else
                                        {
                                            if (remoteTask.Description != workItem.Description)
                                            {
                                                remoteTask.Description = workItem.Description;
                                                changesMade = true;
                                            }
                                        }

                                        //Debug logging - comment out for production code
                                        LogTraceEvent(eventLog, "Got the task name and description\n", EventLogEntryType.Information);

                                        //Now get the task status from the State mapping
                                        dataMapping = FindMappingByExternalKey(projectId, workItem.State, taskStatusMappings, true);
                                        if (dataMapping == null)
                                        {
                                            //We can't find the matching item so log and ignore
                                            eventLog.WriteEntry("Unable to locate mapping entry for Task State " + workItem.State + " in project " + projectId, EventLogEntryType.Error);
                                        }
                                        else
                                        {
                                            if (remoteTask.TaskStatusId != dataMapping.InternalId)
                                            {
                                                remoteTask.TaskStatusId = dataMapping.InternalId;
                                                changesMade = true;
                                            }
                                        }

                                        //Debug logging - comment out for production code
                                        LogTraceEvent(eventLog, "Got the task status\n", EventLogEntryType.Information);

                                        //Update the dates and efforts
                                        try
                                        {
                                            if (workItem["Start Date"] != null)
                                            {
                                                if (remoteTask.StartDate.ToString() != workItem["Start Date"].ToString())
                                                {
                                                    remoteTask.StartDate = (DateTime)workItem["Start Date"];
                                                    changesMade = true;
                                                }
                                            }
                                            if (workItem["Finish Date"] != null)
                                            {
                                                if (remoteTask.EndDate.ToString() != workItem["Finish Date"].ToString())
                                                {
                                                    remoteTask.EndDate = (DateTime)workItem["Finish Date"];
                                                    changesMade = true;
                                                }
                                            }
                                        }
                                        catch (Exception)
                                        {
                                            //Some installations don't let the dates be set externally
                                        }

                                        //Debug logging - comment out for production code
                                        LogTraceEvent(eventLog, "Got the task dates\n", EventLogEntryType.Information);

                                        //Update the actual and estimated work
                                        if (workItem["Completed Work"] != null)
                                        {
                                            double completedWorkHours = (double)workItem["Completed Work"];
                                            int actualEffortMins = (int)(completedWorkHours * (double)60);
                                            if (!remoteTask.EstimatedEffort.HasValue)
                                            {
                                                if (remoteTask.EstimatedEffort.ToString() != actualEffortMins.ToString())
                                                {
                                                    remoteTask.EstimatedEffort = actualEffortMins;
                                                    changesMade = true;
                                                }
                                            }
                                            if (remoteTask.ActualEffort.ToString() != actualEffortMins.ToString())
                                            {
                                                remoteTask.ActualEffort = actualEffortMins;
                                                changesMade = true;
                                            }
                                            
                                            //Calculate the %complete from the remaining work
                                            if (workItem["Remaining Work"] != null)
                                            {
                                                double totalWorkHours = (double)workItem["Remaining Work"] + completedWorkHours;
                                                if (totalWorkHours > 0)
                                                {
                                                    double percentComplete = completedWorkHours / totalWorkHours * 100;
                                                    int percentCompleteInt = (int)percentComplete;
                                                    if (remoteTask.CompletionPercent != percentCompleteInt)
                                                    {
                                                        remoteTask.CompletionPercent = percentCompleteInt;
                                                        changesMade = true;
                                                    }
                                                }
                                            }

                                            //Debug logging - comment out for production code
                                            LogTraceEvent(eventLog, "Got the task efforts\n", EventLogEntryType.Information);
                                        }

                                        //Now we need to see if any of the SpiraTest custom properties that map to TFS fields have changed in TFS
                                        foreach (SpiraImportExport.RemoteCustomProperty customProperty in taskProjectCustomProperties)
                                        {
                                            //First the text fields
                                            if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_TEXT)
                                            {
                                                if (customProperty.Alias == TFS_SPECIAL_FIELD_RANK)
                                                {
                                                    //Now we need to set the value on the SpiraTest task
                                                    changesMade = SetCustomPropertyTextValue(remoteTask, customProperty.CustomPropertyName, (string)workItem["Rank"], changesMade);
                                                }
                                            }

                                            //Next the list fields
                                            if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_LIST)
                                            {
                                                if (customProperty.Alias == TFS_SPECIAL_FIELD_AREA)
                                                {
                                                    //Now we need to set the value on the SpiraTest task
                                                    SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = taskCustomPropertyValueMappingList[customProperty.CustomPropertyId];
                                                    SpiraImportExport.RemoteDataMapping customPropertyValueMapping = FindMappingByExternalKey(projectId, workItem.AreaId.ToString(), customPropertyValueMappings, false);
                                                    if (customPropertyValueMapping != null)
                                                    {
                                                        changesMade = SetCustomPropertyListValue(remoteTask, customProperty.CustomPropertyName, customPropertyValueMapping.InternalId, changesMade);
                                                    }
                                                }
                                                if (customProperty.Alias == TFS_SPECIAL_FIELD_DISCIPLINE && workItem["Discipline"] != null)
                                                {
                                                    //Now we need to set the value on the SpiraTest task
                                                    SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = taskCustomPropertyValueMappingList[customProperty.CustomPropertyId];
                                                    SpiraImportExport.RemoteDataMapping customPropertyValueMapping = FindMappingByExternalKey(projectId, (string)workItem["Discipline"], customPropertyValueMappings, false);
                                                    if (customPropertyValueMapping != null)
                                                    {
                                                        changesMade = SetCustomPropertyListValue(remoteTask, customProperty.CustomPropertyName, customPropertyValueMapping.InternalId, changesMade);
                                                    }
                                                }
                                            }
                                        }

                                        //Debug logging - comment out for production code
                                        LogTraceEvent(eventLog, "Got the task custom properties\n", EventLogEntryType.Information);

                                        //SpiraTest doesn't currently support resolutions in tasks, so ignoring
                                        if (String.IsNullOrEmpty((string)workItem[CoreField.AssignedTo]))
                                        {
                                            if (remoteTask.OwnerId.HasValue)
                                            {
                                                remoteTask.OwnerId = null;
                                                changesMade = true;
                                            }
                                        }
                                        else
                                        {
                                            dataMapping = FindMappingByExternalKey((string)workItem[CoreField.AssignedTo], userMappings);
                                            if (dataMapping == null)
                                            {
                                                //We can't find the matching user so log and ignore
                                                eventLog.WriteEntry("Unable to locate mapping entry for TFS user " + (string)workItem[CoreField.AssignedTo] + " so ignoring the assignee change", EventLogEntryType.Error);
                                            }
                                            else
                                            {
                                                if (!remoteTask.OwnerId.HasValue || remoteTask.OwnerId != dataMapping.InternalId)
                                                {
                                                    remoteTask.OwnerId = dataMapping.InternalId;
                                                    LogTraceEvent(eventLog, "Got the assignee " + remoteTask.OwnerId.ToString() + "\n", EventLogEntryType.Information);
                                                    changesMade = true;
                                                }
                                            }
                                        }

                                        //Specify the resolved-in release if applicable
                                        if (!String.IsNullOrEmpty(workItem.IterationPath))
                                        {
                                            //See if we have a mapped SpiraTest release
                                            dataMapping = FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), releaseMappings, false);
                                            if (dataMapping == null)
                                            {
                                                //Now check to see if recently added
                                                dataMapping = FindMappingByExternalKey(projectId, workItem.IterationId.ToString(), newReleaseMappings.ToArray(), false);
                                            }
                                            if (dataMapping == null)
                                            {
                                                //We can't find the matching item so need to create a new release in SpiraTest and add to mappings

                                                //Need to iterate through the TFS iteration node tree to get the full node object
                                                Node iterationNode = GetMatchingNode(project.IterationRootNodes, workItem.IterationId);
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
                                                    changesMade = true;
                                                }
                                            }
                                            else
                                            {
                                                if (remoteTask.ReleaseId != dataMapping.InternalId)
                                                {
                                                    remoteTask.ReleaseId = dataMapping.InternalId;
                                                    changesMade = true;
                                                }
                                            }
                                        }

                                        //Finally update the task in SpiraTest, exceptions get logged
                                        if (changesMade)
                                        {
                                            spiraImportExport.Task_Update(remoteTask);

                                            //Debug logging - comment out for production code
                                            LogTraceEvent(eventLog, "Successfully updated\n", EventLogEntryType.Information);
                                        }
                                    }
                                }
                                catch (Exception exception)
                                {
                                    //Log and continue execution
                                    eventLog.WriteEntry("Error Synchronizing Tasks between TFS and " + productName + ": " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                                }
                            }
                        }
                    }

                    //Finally we need to update the mapping data on the server
                    //At this point we have potentially added releases
                    spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, ARTIFACT_TYPE_ID_RELEASE, newReleaseMappings.ToArray());
                
                    //Clean up
                    project = null;
                    incidentSeverityMappings = null;
                    incidentPriorityMappings = null;
                    incidentStatusMappings = null;
                    incidentTypeMappings = null;
                    taskPriorityMappings = null;
                    taskStatusMappings = null;
                    incidentProjectCustomProperties = null;
                    incidentCustomPropertyMappingList = null;
                    incidentCustomPropertyValueMappingList = null;
                    taskProjectCustomProperties = null;
                    taskCustomPropertyMappingList = null;
                    taskCustomPropertyValueMappingList = null;
                    incidentMappings = null;
                    taskMappings = null;
                    releaseMappings = null;
                    newIncidentMappings = null;
                    newTaskMappings = null;
                    newReleaseMappings = null;
                }

                //The following code is only needed during debugging
                LogTraceEvent(eventLog, "Import Completed", EventLogEntryType.Warning);

                //Mark objects ready for garbage collection
                spiraImportExport.Dispose();
                teamFoundationServer.Dispose();
                spiraImportExport = null;
                teamFoundationServer = null;
                workItemStore = null;
                cookieContainer = null;
                projectMappings = null;
                userMappings = null;
                credentials = null;
                dataMapping = null;

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
        /// Finds a mapping entry from the internal id and project id
        /// </summary>
        /// <param name="projectId">The project id</param>
        /// <param name="internalId">The internal id</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <returns>The matching entry or Null if none found</returns>
        private SpiraImportExport.RemoteDataMapping FindMappingByInternalId(int projectId, int internalId, SpiraImportExport.RemoteDataMapping[] dataMappings)
        {
            foreach (SpiraImportExport.RemoteDataMapping dataMapping in dataMappings)
            {
                if (dataMapping.InternalId == internalId && dataMapping.ProjectId == projectId)
                {
                    return dataMapping;
                }
            }
            return null;
        }

        /// <summary>
        /// Finds a mapping entry from the external key and project id
        /// </summary>
        /// <param name="projectId">The project id</param>
        /// <param name="externalKey">The external key</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <param name="onlyPrimaryEntries">Do we only want to locate primary entries</param>
        /// <returns>The matching entry or Null if none found</returns>
        private SpiraImportExport.RemoteDataMapping FindMappingByExternalKey(int projectId, string externalKey, SpiraImportExport.RemoteDataMapping[] dataMappings, bool onlyPrimaryEntries)
        {
            foreach (SpiraImportExport.RemoteDataMapping dataMapping in dataMappings)
            {
                if (dataMapping.ExternalKey == externalKey && dataMapping.ProjectId == projectId)
                {
                    //See if we're only meant to return primary entries
                    if (!onlyPrimaryEntries || dataMapping.Primary)
                    {
                        return dataMapping;
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Finds a mapping entry from the internal id
        /// </summary>
        /// <param name="internalId">The internal id</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <returns>The matching entry or Null if none found</returns>
        /// <remarks>Used when no project id stored in the mapping collection</remarks>
        private SpiraImportExport.RemoteDataMapping FindMappingByInternalId(int internalId, SpiraImportExport.RemoteDataMapping[] dataMappings)
        {
            foreach (SpiraImportExport.RemoteDataMapping dataMapping in dataMappings)
            {
                if (dataMapping.InternalId == internalId)
                {
                    return dataMapping;
                }
            }
            return null;
        }

        /// <summary>
        /// Finds a mapping entry from the external key
        /// </summary>
        /// <param name="externalKey">The external key</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <returns>The matching entry or Null if none found</returns>
        /// <remarks>Used when no project id stored in the mapping collection</remarks>
        private SpiraImportExport.RemoteDataMapping FindMappingByExternalKey(string externalKey, SpiraImportExport.RemoteDataMapping[] dataMappings)
        {
            foreach (SpiraImportExport.RemoteDataMapping dataMapping in dataMappings)
            {
                if (dataMapping.ExternalKey == externalKey)
                {
                    return dataMapping;
                }
            }
            return null;
        }

        /// <summary>
        /// Extracts the matching custom property text value from an artifact
        /// </summary>
        /// <param name="remoteArtifact">The artifact</param>
        /// <param name="customPropertyName">The name of the custom property</param>
        /// <returns></returns>
        private String GetCustomPropertyTextValue(SpiraImportExport.RemoteArtifact remoteArtifact, string customPropertyName)
        {
            try
            {
                if (customPropertyName == "TEXT_01")
                {
                    return remoteArtifact.Text01;
                }
                if (customPropertyName == "TEXT_02")
                {
                    return remoteArtifact.Text02;
                }
                if (customPropertyName == "TEXT_03")
                {
                    return remoteArtifact.Text03;
                }
                if (customPropertyName == "TEXT_04")
                {
                    return remoteArtifact.Text04;
                }
                if (customPropertyName == "TEXT_05")
                {
                    return remoteArtifact.Text05;
                }
                if (customPropertyName == "TEXT_06")
                {
                    return remoteArtifact.Text06;
                }
                if (customPropertyName == "TEXT_07")
                {
                    return remoteArtifact.Text07;
                }
                if (customPropertyName == "TEXT_08")
                {
                    return remoteArtifact.Text08;
                }
                if (customPropertyName == "TEXT_09")
                {
                    return remoteArtifact.Text09;
                }
                if (customPropertyName == "TEXT_10")
                {
                    return remoteArtifact.Text10;
                }
                return null;
            }
            catch (Exception exception)
            {
                eventLog.WriteEntry("Unable to get custom property text value: " + customPropertyName, EventLogEntryType.Error);
                throw exception;
            }
        }

        /// <summary>
        /// Sets the matching custom property text value on an artifact
        /// </summary>
        /// <param name="remoteArtifact">The artifact</param>
        /// <param name="customPropertyName">The name of the custom property</param>
        /// <param name="value">The value to set</param>
        private void SetCustomPropertyTextValue(SpiraImportExport.RemoteArtifact remoteArtifact, string customPropertyName, string value)
        {
            this.SetCustomPropertyTextValue(remoteArtifact, customPropertyName, value, false);
        }

        /// <summary>
        /// Sets the matching custom property text value on an artifact
        /// </summary>
        /// <param name="remoteArtifact">The artifact</param>
        /// <param name="customPropertyName">The name of the custom property</param>
        /// <param name="value">The value to set</param>
        /// <param name="changesMade">If any changes have been made to the artifact</param>
        /// <returns>If any changes were made</returns>
        private bool SetCustomPropertyTextValue(SpiraImportExport.RemoteArtifact remoteArtifact, string customPropertyName, string value, bool changesMade)
        {
            if (customPropertyName == "TEXT_01")
            {
                if (remoteArtifact.Text01 != value)
                {
                    changesMade = true;
                    remoteArtifact.Text01 = value;
                }
            }
            if (customPropertyName == "TEXT_02")
            {
                if (remoteArtifact.Text02 != value)
                {
                    changesMade = true;
                    remoteArtifact.Text02 = value;
                }
            }
            if (customPropertyName == "TEXT_03")
            {
                if (remoteArtifact.Text03 != value)
                {
                    changesMade = true;
                    remoteArtifact.Text03 = value;
                }
            }
            if (customPropertyName == "TEXT_04")
            {
                if (remoteArtifact.Text04 != value)
                {
                    changesMade = true;
                    remoteArtifact.Text04 = value;
                }
            }
            if (customPropertyName == "TEXT_05")
            {
                if (remoteArtifact.Text05 != value)
                {
                    changesMade = true;
                    remoteArtifact.Text05 = value;
                }
            }
            if (customPropertyName == "TEXT_06")
            {
                if (remoteArtifact.Text06 != value)
                {
                    changesMade = true;
                    remoteArtifact.Text06 = value;
                }
            }
            if (customPropertyName == "TEXT_07")
            {
                if (remoteArtifact.Text07 != value)
                {
                    changesMade = true;
                    remoteArtifact.Text07 = value;
                }
            }
            if (customPropertyName == "TEXT_08")
            {
                if (remoteArtifact.Text08 != value)
                {
                    changesMade = true;
                    remoteArtifact.Text08 = value;
                }
            }
            if (customPropertyName == "TEXT_09")
            {
                if (remoteArtifact.Text09 != value)
                {
                    changesMade = true;
                    remoteArtifact.Text09 = value;
                }
            }
            if (customPropertyName == "TEXT_10")
            {
                if (remoteArtifact.Text10 != value)
                {
                    changesMade = true;
                    remoteArtifact.Text10 = value;
                }
            }
            return changesMade;
        }

        /// <summary>
        /// Sets the matching custom property list value on an artifact
        /// </summary>
        /// <param name="remoteArtifact">The artifact</param>
        /// <param name="customPropertyName">The name of the custom property</param>
        /// <param name="value">The value to set</param>
        private void SetCustomPropertyListValue(SpiraImportExport.RemoteArtifact remoteArtifact, string customPropertyName, Nullable<int> value)
        {
            this.SetCustomPropertyListValue(remoteArtifact, customPropertyName, value, false);
        }

        /// <summary>
        /// Sets the matching custom property list value on an artifact
        /// </summary>
        /// <param name="remoteArtifact">The artifact</param>
        /// <param name="customPropertyName">The name of the custom property</param>
        /// <param name="value">The value to set</param>
        /// <param name="changesMade">Have any other changes been made to the artifact</param>
        /// <returns>If any changes were made</returns>
        private bool SetCustomPropertyListValue(SpiraImportExport.RemoteArtifact remoteArtifact, string customPropertyName, Nullable<int> value, bool changesMade)
        {
            if (customPropertyName == "LIST_01")
            {
                if (remoteArtifact.List01.ToString() != value.ToString())
                {
                    changesMade = true;
                    remoteArtifact.List01 = value;
                }
            }
            if (customPropertyName == "LIST_02")
            {
                if (remoteArtifact.List02.ToString() != value.ToString())
                {
                    changesMade = true;
                    remoteArtifact.List02 = value;
                }

            }
            if (customPropertyName == "LIST_03")
            {
                if (remoteArtifact.List03.ToString() != value.ToString())
                {
                    changesMade = true;
                    remoteArtifact.List03 = value;
                }

            }
            if (customPropertyName == "LIST_04")
            {
                if (remoteArtifact.List04.ToString() != value.ToString())
                {
                    changesMade = true;
                    remoteArtifact.List04 = value;
                }

            }
            if (customPropertyName == "LIST_05")
            {
                if (remoteArtifact.List05.ToString() != value.ToString())
                {
                    changesMade = true;
                    remoteArtifact.List05 = value;
                }

            }
            if (customPropertyName == "LIST_06")
            {
                if (remoteArtifact.List06.ToString() != value.ToString())
                {
                    changesMade = true;
                    remoteArtifact.List06 = value;
                }

            }
            if (customPropertyName == "LIST_07")
            {
                if (remoteArtifact.List07.ToString() != value.ToString())
                {
                    changesMade = true;
                    remoteArtifact.List07 = value;
                }

            }
            if (customPropertyName == "LIST_08")
            {
                if (remoteArtifact.List08.ToString() != value.ToString())
                {
                    changesMade = true;
                    remoteArtifact.List08 = value;
                }

            }
            if (customPropertyName == "LIST_09")
            {
                if (remoteArtifact.List09.ToString() != value.ToString())
                {
                    changesMade = true;
                    remoteArtifact.List09 = value;
                }

            }
            if (customPropertyName == "LIST_10")
            {
                if (remoteArtifact.List10.ToString() != value.ToString())
                {
                    changesMade = true;
                    remoteArtifact.List10 = value;
                }

            }
            return changesMade;
        }


        /// <summary>
        /// Extracts the matching custom property list value from an artifact
        /// </summary>
        /// <param name="remoteArtifact">The artifact</param>
        /// <param name="customPropertyName">The name of the custom property</param>
        /// <returns></returns>
        private Nullable<int> GetCustomPropertyListValue(SpiraImportExport.RemoteArtifact remoteArtifact, string customPropertyName)
        {
            try
            {
                if (customPropertyName == "LIST_01")
                {
                    return remoteArtifact.List01;
                }
                if (customPropertyName == "LIST_02")
                {
                    return remoteArtifact.List02;
                }
                if (customPropertyName == "LIST_03")
                {
                    return remoteArtifact.List03;
                }
                if (customPropertyName == "LIST_04")
                {
                    return remoteArtifact.List04;
                }
                if (customPropertyName == "LIST_05")
                {
                    return remoteArtifact.List05;
                }
                if (customPropertyName == "LIST_06")
                {
                    return remoteArtifact.List06;
                }
                if (customPropertyName == "LIST_07")
                {
                    return remoteArtifact.List07;
                }
                if (customPropertyName == "LIST_08")
                {
                    return remoteArtifact.List08;
                }
                if (customPropertyName == "LIST_09")
                {
                    return remoteArtifact.List09;
                }
                if (customPropertyName == "LIST_10")
                {
                    return remoteArtifact.List10;
                }
                return null;
            }
            catch (Exception exception)
            {
                eventLog.WriteEntry("Unable to get custom property list value: " + customPropertyName, EventLogEntryType.Error);
                throw exception;
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
		/// Renders HTML content as plain text, since MSTFS cannot handle tags
		/// </summary>
		/// <param name="source">The HTML markup</param>
		/// <returns>Plain text representation</returns>
		/// <remarks>Handles line-breaks, etc.</remarks>
		protected string HtmlRenderAsPlainText (string source)
		{
			try
			{
				string result;

				// Remove HTML Development formatting
				// Replace line breaks with space
				// because browsers inserts space
				result = source.Replace("\r", " ");
				// Replace line breaks with space
				// because browsers inserts space
				result = result.Replace("\n", " ");
				// Remove step-formatting
				result = result.Replace("\t", string.Empty);
				// Remove repeating speces becuase browsers ignore them
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"( )+", " ");

				// Remove the header (prepare first by clearing attributes)
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"<( )*head([^>])*>","<head>", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"(<( )*(/)( )*head( )*>)","</head>", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					"(<head>).*(</head>)",string.Empty, 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);

				// remove all scripts (prepare first by clearing attributes)
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"<( )*script([^>])*>","<script>", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"(<( )*(/)( )*script( )*>)","</script>", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				//result = System.Text.RegularExpressions.Regex.Replace(result, 
				//         @"(<script>)([^(<script>\.</script>)])*(</script>)",
				//         string.Empty, 
				//         System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"(<script>).*(</script>)",string.Empty, 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        
				// remove all styles (prepare first by clearing attributes)
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"<( )*style([^>])*>","<style>", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"(<( )*(/)( )*style( )*>)","</style>", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					"(<style>).*(</style>)",string.Empty, 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);

				// insert tabs in spaces of <td> tags
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"<( )*td([^>])*>","\t", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);

				// insert line breaks in places of <BR> and <LI> tags
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"<( )*br( )*>","\r", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"<( )*li( )*>","\r", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);

				// insert line paragraphs (double line breaks) in place
				// if <P>, <DIV> and <TR> tags
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"<( )*div([^>])*>","\r\r", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"<( )*tr([^>])*>","\r\r", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"<( )*p([^>])*>","\r\r", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);

				// Remove remaining tags like <a>, links, images,
				// comments etc - anything thats enclosed inside < >
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"<[^>]*>",string.Empty, 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);

				// replace special characters:
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"&nbsp;"," ", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"&bull;"," * ", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);    
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"&lsaquo;","<", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);        
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"&rsaquo;",">", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);        
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"&trade;","(tm)", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);        
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"&frasl;","/", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);        
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"<","<", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);        
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@">",">", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);        
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"&copy;","(c)", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);        
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"&reg;","(r)", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);    
				// Remove all others. More can be added, see
				// http://hotwired.lycos.com/webmonkey/reference/special_characters/
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					@"&(.{2,6});", string.Empty, 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);    

				// for testng
				//System.Text.RegularExpressions.Regex.Replace(result, 
				//       this.txtRegex.Text,string.Empty, 
				//       System.Text.RegularExpressions.RegexOptions.IgnoreCase);

				// make line breaking consistent
				result = result.Replace("\n", "\r");

				// Remove extra line breaks and tabs:
				// replace over 2 breaks with 2 and over 4 tabs with 4. 
				// Prepare first to remove any whitespaces inbetween
				// the escaped characters and remove redundant tabs inbetween linebreaks
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					"(\r)( )+(\r)","\r\r", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					"(\t)( )+(\t)","\t\t", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					"(\t)( )+(\r)","\t\r", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					"(\r)( )+(\t)","\r\t", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				// Remove redundant tabs
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					"(\r)(\t)+(\r)","\r\r", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				// Remove multible tabs followind a linebreak with just one tab
				result = System.Text.RegularExpressions.Regex.Replace(result, 
					"(\r)(\t)+","\r\t", 
					System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				// Initial replacement target string for linebreaks
				string breaks = "\r\r\r";
				// Initial replacement target string for tabs
				string tabs = "\t\t\t\t\t";
				for (int index=0; index<result.Length; index++)
				{
					result = result.Replace(breaks, "\r\r");
					result = result.Replace(tabs, "\t\t\t\t");
					breaks = breaks + "\r";    
					tabs = tabs + "\t";
				}

				// Thats it.
				return result;

			}
			catch
			{
				return source;
			}
		}

        /// <summary>
        /// Adds a new iteration to TFS
        /// </summary>
        /// <param name="project"></param>
        /// <param name="iterationName">The name of the new iteration</param>
        /// <returns>The new iteration node</returns>
        /// <remarks>The new iteration is added under the root node</remarks>
        private Node AddNewTfsIteration(TeamFoundationServer tfs, ref WorkItemStore workItemStore, ref Project project, string iterationName)
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
            ICommonStructureService commonStructureService = (ICommonStructureService)tfs.GetService(typeof(ICommonStructureService));
            string nodeUri = commonStructureService.CreateNode(iterationName, parentNodeUri);

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
		protected virtual void Dispose (bool disposing)
		{
			// Check to see if Dispose has already been called.
			if(!this.disposed)
			{
				// If disposing equals true, dispose all managed 
				// and unmanaged resources.
				if(disposing)
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
        protected void LogTraceEvent(EventLog eventLog, string message, EventLogEntryType type)
        {
            if (traceLogging)
            {
                eventLog.WriteEntry(message, type);
            }
        }
	}

    /// <summary>
    /// Returns the credentials that we should be using
    /// </summary>
    public class DataSyncCredentialsProvider : ICredentialsProvider
    {
        string msTfsLogin, msTfsPassword, msTfsDomain;

        public DataSyncCredentialsProvider(string msTfsLogin, string msTfsPassword, string msTfsDomain)
        {
            this.msTfsDomain = msTfsDomain;
            this.msTfsLogin = msTfsLogin;
            this.msTfsPassword = msTfsPassword;
        }

        public ICredentials GetCredentials(Uri uri, ICredentials failedCredentials)
        {
            //Configure the network credentials - used for accessing the MsTfs API
            ICredentials credentials = new NetworkCredential(msTfsLogin, msTfsPassword, msTfsDomain);
            return credentials;
        }

        public void NotifyCredentialsAuthenticated(Uri uri)
        {
            // Do nothing
        }
    }
}
