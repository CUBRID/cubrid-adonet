using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using CUBRID.Data.CUBRIDClient;
using System.IO;

namespace CUBRID.Data.Test.UI
{
	public partial class CubridForm : Form
	{
		public int selected;

		public CubridForm()
		{
			selected = 0;
			InitializeComponent();

			conn.Open();

			cmdStadium.Connection = conn;
			cmdGames.Connection = conn;

			DataTable dt1 = new DataTable("stadium");
			CUBRIDDataAdapterStadium.Fill(dt1);
			CUBRIDDataSet.Tables.Add(dt1);

			DataTable dt2 = new DataTable("game");
			CUBRIDDataAdapterGames.Fill(dt2);
			CUBRIDDataSet.Tables.Add(dt2);
		}

		private void CubridForm_Load(object sender, EventArgs e)
		{
			CUBRIDDataSet.Relations.Add("PK",
					CUBRIDDataSet.Tables[0].Columns["code"],
					CUBRIDDataSet.Tables[1].Columns["stadium_code"]);

			stadiumGrid.DataSource = CUBRIDDataSet.Tables[0];
		}

		private void stadiumGrid_CellContentClick(object sender, DataGridViewCellEventArgs e)
		{
			gameGrid.DataSource = CUBRIDDataSet.Tables["stadium"];
			gameGrid.DataMember = "PK";
		}


	}
}
