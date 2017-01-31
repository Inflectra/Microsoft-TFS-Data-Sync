using System;
using System.Drawing;
using System.Collections;
using System.ComponentModel;
using System.Windows.Forms;
using System.Data;
using System.Text;

using Microsoft.TeamFoundation;
using Microsoft.TeamFoundation.Client;
using Microsoft.TeamFoundation.Common;
using Microsoft.TeamFoundation.WorkItemTracking;
using Microsoft.TeamFoundation.WorkItemTracking.Client;

namespace TestForm
{
	/// <summary>
	/// Summary description for Form1.
	/// </summary>
	public class Form1 : System.Windows.Forms.Form
	{
		private System.Windows.Forms.Button button1;
		private System.Windows.Forms.TextBox textBox1;
		private System.Windows.Forms.Button button2;
		private System.Windows.Forms.TextBox txtBugId;
		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.TextBox txtDomain;
		private System.Windows.Forms.TextBox txtLogin;
		private System.Windows.Forms.Label label2;
		private System.Windows.Forms.TextBox txtPassword;
		/// <summary>
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.Container components = null;

		public Form1()
		{
			//
			// Required for Windows Form Designer support
			//
			InitializeComponent();
		}

		/// <summary>
		/// Clean up any resources being used.
		/// </summary>
		protected override void Dispose( bool disposing )
		{
			if( disposing )
			{
				if (components != null) 
				{
					components.Dispose();
				}
			}
			base.Dispose( disposing );
		}

		#region Windows Form Designer generated code
		/// <summary>
		/// Required method for Designer support - do not modify
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
			this.button1 = new System.Windows.Forms.Button();
			this.textBox1 = new System.Windows.Forms.TextBox();
			this.button2 = new System.Windows.Forms.Button();
			this.txtBugId = new System.Windows.Forms.TextBox();
			this.label1 = new System.Windows.Forms.Label();
			this.txtDomain = new System.Windows.Forms.TextBox();
			this.txtLogin = new System.Windows.Forms.TextBox();
			this.label2 = new System.Windows.Forms.Label();
			this.txtPassword = new System.Windows.Forms.TextBox();
			this.SuspendLayout();
			// 
			// button1
			// 
			this.button1.Location = new System.Drawing.Point(24, 48);
			this.button1.Name = "button1";
			this.button1.Size = new System.Drawing.Size(280, 56);
			this.button1.TabIndex = 0;
			this.button1.Text = "Get Work Item";
			this.button1.Click += new System.EventHandler(this.button1_Click);
			// 
			// textBox1
			// 
			this.textBox1.Location = new System.Drawing.Point(16, 120);
			this.textBox1.Multiline = true;
			this.textBox1.Name = "textBox1";
			this.textBox1.Size = new System.Drawing.Size(528, 224);
			this.textBox1.TabIndex = 1;
			this.textBox1.Text = "";
			// 
			// button2
			// 
			this.button2.Location = new System.Drawing.Point(320, 48);
			this.button2.Name = "button2";
			this.button2.Size = new System.Drawing.Size(208, 56);
			this.button2.TabIndex = 2;
			this.button2.Text = "Create Work Item";
			this.button2.Click += new System.EventHandler(this.button2_Click);
			// 
			// txtBugId
			// 
			this.txtBugId.Location = new System.Drawing.Point(72, 16);
			this.txtBugId.Name = "txtBugId";
			this.txtBugId.TabIndex = 3;
			this.txtBugId.Text = "";
			// 
			// label1
			// 
			this.label1.Location = new System.Drawing.Point(24, 16);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(40, 16);
			this.label1.TabIndex = 4;
			this.label1.Text = "Bug #:";
			// 
			// txtDomain
			// 
			this.txtDomain.Location = new System.Drawing.Point(200, 16);
			this.txtDomain.Name = "txtDomain";
			this.txtDomain.TabIndex = 5;
			this.txtDomain.Text = "domain";
			// 
			// txtLogin
			// 
			this.txtLogin.Location = new System.Drawing.Point(312, 16);
			this.txtLogin.Name = "txtLogin";
			this.txtLogin.TabIndex = 6;
			this.txtLogin.Text = "login";
			// 
			// label2
			// 
			this.label2.Location = new System.Drawing.Point(304, 16);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(16, 23);
			this.label2.TabIndex = 7;
			this.label2.Text = "/";
			// 
			// txtPassword
			// 
			this.txtPassword.Location = new System.Drawing.Point(416, 16);
			this.txtPassword.Name = "txtPassword";
			this.txtPassword.TabIndex = 8;
			this.txtPassword.Text = "password";
			// 
			// Form1
			// 
			this.AutoScaleBaseSize = new System.Drawing.Size(5, 13);
			this.ClientSize = new System.Drawing.Size(552, 359);
			this.Controls.Add(this.txtLogin);
			this.Controls.Add(this.txtPassword);
			this.Controls.Add(this.label2);
			this.Controls.Add(this.txtDomain);
			this.Controls.Add(this.label1);
			this.Controls.Add(this.txtBugId);
			this.Controls.Add(this.button2);
			this.Controls.Add(this.textBox1);
			this.Controls.Add(this.button1);
			this.Name = "Form1";
			this.Text = "Form1";
			this.ResumeLayout(false);

		}
		#endregion

		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		[STAThread]
		static void Main() 
		{
			Application.Run(new Form1());
		}

		private void button1_Click(object sender, System.EventArgs e)
		{
			//Configure credentials
			ICredentials credentials = new NetworkCredential(this.txtLogin.Text, this.txtPassword.Text, this.txtDomain.Text);
            
            //Create the team foundation server class
            TeamFoundationServer teamFoundationServer = new TeamFoundationServer("http://inflectrasvr03:8080/", credentials);

            //Get access to the work item store
            WorkItemStore workItemStore = new WorkItemStore(teamFoundationServer);

            //Now get the work item by id
            int workItemId = Int32.Parse(this.txtBugId.Text);
            WorkItem workItem = workItemStore.GetWorkItem(workItemId);

            //Populate the work item info
			this.textBox1.Text = workItem.Title + " - " + workItem.Description;
		}

		private void button2_Click(object sender, System.EventArgs e)
		{
            //Configure credentials
            ICredentials credentials = new NetworkCredential(this.txtLogin.Text, this.txtPassword.Text, this.txtDomain.Text);

            //Create the team foundation server class
            TeamFoundationServer teamFoundationServer = new TeamFoundationServer("http://inflectrasvr03:8080/", credentials);

            //Get access to the work item store
            WorkItemStore workItemStore = new WorkItemStore(teamFoundationServer);

            try
            {
                //Create the new work item
                Project project = workItemStore.Projects["Library Information System"];
                WorkItemType workItemType = project.WorkItemTypes["Bug"];
                WorkItem workItem = new WorkItem(workItemType);
                //workItem.AreaId = 1;
                workItem.Title = "SpiraTest Imported bug";
                workItem.Description = "Test description from SpiraTest";
                workItem.State = "Active";
                workItem.Reason = "New";
                workItem["Issue"] = "No";
                //workItem[CoreField.CreatedBy] = "Administrator";
                //workItem[CoreField.ChangedDate] = DateTime.Now;
                workItem.Save();
                this.textBox1.Text = workItem.Id.ToString();
            }
            catch (Exception exception)
            {
                this.textBox1.Text = exception.Message + "\nStack Trace:\n" + exception.StackTrace;
            }
		}
	}
}
