using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Microsoft.TeamFoundation.Client;
using System.Net;
using Microsoft.TeamFoundation.WorkItemTracking.Client;
using System.Diagnostics;
using Microsoft.TeamFoundation.Framework.Client;
using Microsoft.TeamFoundation.Framework.Common;
using Microsoft.TeamFoundation.WorkItemTracking.Proxy;

namespace TestForm
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void btnConnect_Click(object sender, EventArgs e)
        {
            string connectionString = this.txtUrl.Text;
            string windowsDomain = this.txtDomain.Text;
            string externalLogin = this.txtLogin.Text;
            string externalPassword = this.txtPassword.Text;

            //Configure the network credentials - used for accessing the MsTfs API
            //If we have a domain provided, use a NetworkCredential, otherwise use a TFS credential
            TfsClientCredentials tfsCredential = null;
            NetworkCredential networkCredential = null;
            if (String.IsNullOrEmpty(windowsDomain))
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
                networkCredential = new NetworkCredential(externalLogin, externalPassword, windowsDomain);
            }

            //Create a new TFS 2012 project collection instance and WorkItemStore instance
            //This requires that the URI includes the collection name not just the server name
            WorkItemStore workItemStore = null;
            Uri tfsUri = new Uri(connectionString);
            TfsTeamProjectCollection tfsTeamProjectCollection;
            if (String.IsNullOrEmpty(windowsDomain))
            {
                tfsTeamProjectCollection = new TfsTeamProjectCollection(tfsUri, tfsCredential);
                tfsTeamProjectCollection.Authenticate();
            }
            else
            {
                tfsTeamProjectCollection = new TfsTeamProjectCollection(tfsUri, networkCredential);
            }
            LogTraceEvent("Created new TFS Project Collection instance");

            //Get access to the work item server service
            WorkItemServer workItemServer = tfsTeamProjectCollection.GetService<WorkItemServer>();
            LogTraceEvent("Got access to the WorkItemServer service");

            //Get the global security service to retrieve the TFS user list
            TeamFoundationIdentity[] tfsUsers;
            IIdentityManagementService ims = (IIdentityManagementService)tfsTeamProjectCollection.GetService(typeof(IIdentityManagementService));
            if (ims != null)
            {
                TeamFoundationIdentity SIDS = ims.ReadIdentity(IdentitySearchFactor.AccountName, "Project Collection Valid Users", MembershipQuery.None, ReadIdentityOptions.IncludeReadFromSource);
                if (SIDS != null)
                {
                    tfsUsers = ims.ReadIdentities(SIDS.Members, MembershipQuery.Expanded, ReadIdentityOptions.ExtendedProperties);
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
                LogErrorEvent("Unable to connect to Team Foundation Server, please check that the connection information is correct (" + exception.Message + ")", EventLogEntryType.Error);
                LogErrorEvent(exception.Message + ": " + exception.StackTrace);
                if (exception.InnerException != null)
                {
                    LogErrorEvent("Inner Exception=" + exception.InnerException.Message + ": " + exception.InnerException.StackTrace);
                }
            }
            if (workItemStore == null)
            {
                //We can't authenticate so end
                LogErrorEvent("Unable to connect to Team Foundation Server, please check that the connection information is correct", EventLogEntryType.Error);
            }
            LogTraceEvent("Got access to the WorkItemStore");
        }

        protected void LogTraceEvent(string message)
        {
            MessageBox.Show(message, "Trace Event", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }

        protected void LogErrorEvent(string message)
        {
            MessageBox.Show(message, "Error Event", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
        }

        protected void LogErrorEvent(string message, EventLogEntryType type)
        {
            MessageBox.Show(message, "Error Event", MessageBoxButtons.OK, (type == EventLogEntryType.Error) ? MessageBoxIcon.Error : MessageBoxIcon.Warning);
        }
    }
}
