using CUBRID.Data;

namespace CUBRID.Data.Test.UI
{
		partial class CubridForm
		{
				/// <summary>
				/// Required designer variable.
				/// </summary>
				private System.ComponentModel.IContainer components = null;

				/// <summary>
				/// Clean up any resources being used.
				/// </summary>
				/// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
				protected override void Dispose(bool disposing)
				{
						if (disposing && (components != null))
						{
								components.Dispose();
						}
						base.Dispose(disposing);
				}

				#region Windows Form Designer generated code

				/// <summary>
				/// Required method for Designer support - do not modify
				/// the contents of this method with the code editor.
				/// </summary>
				private void InitializeComponent()
				{
			System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle3 = new System.Windows.Forms.DataGridViewCellStyle();
			System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle4 = new System.Windows.Forms.DataGridViewCellStyle();
			this.conn = new CUBRID.Data.CUBRIDClient.CUBRIDConnection();
			this.cmdStadium = new CUBRID.Data.CUBRIDClient.CUBRIDCommand();
			this.cmdGames = new CUBRID.Data.CUBRIDClient.CUBRIDCommand();
			this.CUBRIDDataAdapterStadium = new CUBRID.Data.CUBRIDClient.CUBRIDDataAdapter();
			this.CUBRIDDataAdapterGames = new CUBRID.Data.CUBRIDClient.CUBRIDDataAdapter();
			this.CUBRIDDataSet = new System.Data.DataSet();
			this.stadiumGrid = new System.Windows.Forms.DataGridView();
			this.gameGrid = new System.Windows.Forms.DataGridView();
			this.label1 = new System.Windows.Forms.Label();
			this.label2 = new System.Windows.Forms.Label();
			this.label3 = new System.Windows.Forms.Label();
			((System.ComponentModel.ISupportInitialize)(this.CUBRIDDataSet)).BeginInit();
			((System.ComponentModel.ISupportInitialize)(this.stadiumGrid)).BeginInit();
			((System.ComponentModel.ISupportInitialize)(this.gameGrid)).BeginInit();
			this.SuspendLayout();
			// 
			// conn
			// 
            this.conn.ConnectionString = "server=test-db-server;database=demodb;port=33000;user=public;password=";
			this.conn.DbVersion = "";
			// 
			// cmdStadium
			// 
			this.cmdStadium.CommandText = "SELECT * FROM stadium";
			this.cmdStadium.CommandTimeout = 15;
			// 
			// cmdGames
			// 
			this.cmdGames.CommandText = "SELECT *  FROM game";
			this.cmdGames.CommandTimeout = 15;
			// 
			// CUBRIDDataAdapterStadium
			// 
			this.CUBRIDDataAdapterStadium.DeleteCommand = null;
			this.CUBRIDDataAdapterStadium.InsertCommand = null;
			this.CUBRIDDataAdapterStadium.SelectCommand = this.cmdStadium;
			this.CUBRIDDataAdapterStadium.UpdateCommand = null;
			// 
			// CUBRIDDataAdapterGames
			// 
			this.CUBRIDDataAdapterGames.DeleteCommand = null;
			this.CUBRIDDataAdapterGames.InsertCommand = null;
			this.CUBRIDDataAdapterGames.SelectCommand = this.cmdGames;
			this.CUBRIDDataAdapterGames.UpdateCommand = null;
			// 
			// CUBRIDDataSet
			// 
			this.CUBRIDDataSet.DataSetName = "CUBRIDDataSet";
			// 
			// stadiumGrid
			// 
			this.stadiumGrid.AllowUserToAddRows = false;
			this.stadiumGrid.AllowUserToDeleteRows = false;
			this.stadiumGrid.AllowUserToOrderColumns = true;
			this.stadiumGrid.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.DisplayedCells;
			this.stadiumGrid.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
			this.stadiumGrid.Location = new System.Drawing.Point(12, 51);
			this.stadiumGrid.Name = "stadiumGrid";
			dataGridViewCellStyle3.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
			dataGridViewCellStyle3.BackColor = System.Drawing.SystemColors.Control;
			dataGridViewCellStyle3.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			dataGridViewCellStyle3.ForeColor = System.Drawing.SystemColors.Desktop;
			dataGridViewCellStyle3.SelectionBackColor = System.Drawing.SystemColors.Highlight;
			dataGridViewCellStyle3.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
			dataGridViewCellStyle3.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
			this.stadiumGrid.RowHeadersDefaultCellStyle = dataGridViewCellStyle3;
			this.stadiumGrid.Size = new System.Drawing.Size(715, 212);
			this.stadiumGrid.TabIndex = 0;
			this.stadiumGrid.CellContentClick += new System.Windows.Forms.DataGridViewCellEventHandler(this.stadiumGrid_CellContentClick);
			// 
			// gameGrid
			// 
			this.gameGrid.AllowUserToAddRows = false;
			this.gameGrid.AllowUserToDeleteRows = false;
			this.gameGrid.AllowUserToOrderColumns = true;
			this.gameGrid.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.DisplayedCells;
			this.gameGrid.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
			this.gameGrid.Location = new System.Drawing.Point(12, 306);
			this.gameGrid.Name = "gameGrid";
			dataGridViewCellStyle4.ForeColor = System.Drawing.Color.Purple;
			this.gameGrid.RowsDefaultCellStyle = dataGridViewCellStyle4;
			this.gameGrid.Size = new System.Drawing.Size(715, 168);
			this.gameGrid.TabIndex = 1;
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.label1.ForeColor = System.Drawing.SystemColors.InactiveCaptionText;
			this.label1.Location = new System.Drawing.Point(13, 9);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(300, 13);
			this.label1.TabIndex = 2;
			this.label1.Text = "(Click on the Stadium name to view the corresponding Games)";
			// 
			// label2
			// 
			this.label2.AutoSize = true;
			this.label2.ForeColor = System.Drawing.SystemColors.HotTrack;
			this.label2.Location = new System.Drawing.Point(13, 35);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(48, 13);
			this.label2.TabIndex = 3;
			this.label2.Text = "Stadium:";
			// 
			// label3
			// 
			this.label3.AutoSize = true;
			this.label3.ForeColor = System.Drawing.SystemColors.HotTrack;
			this.label3.Location = new System.Drawing.Point(13, 290);
			this.label3.Name = "label3";
			this.label3.Size = new System.Drawing.Size(43, 13);
			this.label3.TabIndex = 4;
			this.label3.Text = "Games:";
			// 
			// CubridForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(739, 498);
			this.Controls.Add(this.label3);
			this.Controls.Add(this.label2);
			this.Controls.Add(this.label1);
			this.Controls.Add(this.gameGrid);
			this.Controls.Add(this.stadiumGrid);
			this.Name = "CubridForm";
			this.Text = "CUBRID ADO.NET Demo";
			this.Load += new System.EventHandler(this.CubridForm_Load);
			((System.ComponentModel.ISupportInitialize)(this.CUBRIDDataSet)).EndInit();
			((System.ComponentModel.ISupportInitialize)(this.stadiumGrid)).EndInit();
			((System.ComponentModel.ISupportInitialize)(this.gameGrid)).EndInit();
			this.ResumeLayout(false);
			this.PerformLayout();

				}

				#endregion

				public CUBRIDClient.CUBRIDConnection conn;
				public CUBRIDClient.CUBRIDCommand cmdStadium;
				private CUBRIDClient.CUBRIDCommand cmdGames;
				public CUBRIDClient.CUBRIDDataAdapter CUBRIDDataAdapterStadium;
				public CUBRIDClient.CUBRIDDataAdapter CUBRIDDataAdapterGames;
				private System.Data.DataSet CUBRIDDataSet;
				private System.Windows.Forms.DataGridView stadiumGrid;
				private System.Windows.Forms.DataGridView gameGrid;
				private System.Windows.Forms.Label label1;
				private System.Windows.Forms.Label label2;
				private System.Windows.Forms.Label label3;

		}
}

