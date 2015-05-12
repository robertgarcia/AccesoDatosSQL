//-- ===============================================================================
//-- Author: Roberto D. Garcia
//-- Empresa : SYTCORP
//-- Create date: 11/06/2015
//-- Description: Genera metodos para facilitar el acceso a los datos en SQL SERVER.
//-- ================================================================================

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace AccesoDatos
{
    public class AccesoSQL
    {
        //Variables para la conexion al servidor.
        public string cConex = "Data Source=.;Initial Catalog=siga;User=admin;Password=admin";
        public SqlConnection conexion;

        public void AccesoDatos()
        {
            this.conexion = new SqlConnection(this.cConex);
        }

        /// <summary>
        /// Abre la conexion con la base de datos.
        /// </summary>
        public void AbrirConexion()
        {
            try
            {
                //Verifica el estado de la conexion.
                if (this.conexion.State != ConnectionState.Broken && this.conexion.State != ConnectionState.Closed)
                {
                    return;
                }
                this.conexion.Open(); //Si todo sale bien abre la conexion.
            }
            catch (Exception ex)
            {
                throw new Exception("Error al Conectarse al servidor de Base de Datos - " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Cerra la conexion con la base de datos.
        /// </summary>
        public void CerrarConexion()
        {
            try
            {
                //Verifica que la conexion no este cerrada
                if (this.conexion.State != ConnectionState.Open)
                { return; }

                this.conexion.Close(); //Si la conexion esta abierta la cerramos.
            }
            catch
            {

            }
        }
        
        /// <summary>
        /// Crea un comando tipo StoredProcedure al pasarle como paremetro el nombre del SP.
        /// </summary>
        /// <param name="vSP">Nombre del Stored Procedure.</param>
        /// <returns>Retorna un comando de tipo StoredProcedure.</returns>
        public SqlCommand CrearComando(string vSP)
        {
            SqlCommand sqlcommand = new SqlCommand(vSP, this.conexion); //Pasamos el procedimento y la conexion
            sqlcommand.CommandType = CommandType.StoredProcedure; //Se define el tipo comando que sera
            return sqlcommand; //Retornamos el sqlcommand
        }

        /// <summary>
        /// Metodo que devuelve un DataSet al enviar como parametro el Stored Procedure y el comando SQL.
        /// </summary>
        /// <param name="vcmd">Comando SQL</param>
        /// <param name="vSP">Nombre del Stored Procedure</param>
        /// <returns>Devuelve un DataSet.</returns>
        public DataSet GetDS(SqlCommand vcmd, string vSP)
        {
            DataSet dataSet = new DataSet();
            try
            {
                new SqlDataAdapter(vcmd).Fill(dataSet, vSP); //Se carga el query y llena el dataset que se retornara.
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Error Devolviendo DataSet - {0} - {1}", (object)vSP, (object)ex.Message), ex);
            }
            finally
            {
                vcmd.Connection.Close();
                vcmd.Dispose();
            }
            return dataSet;
        }

        /// <summary>
        /// Ejecuta un accion mediante un SqlCommand que se le envia como parametro.
        /// </summary>
        /// <param name="comando">Comando con la accion que se queria ejecutar</param>
        /// <returns>Retorna el numero de filas afectadas por la accion</returns>
        public int Ejecuta_Accion(ref SqlCommand comando)
        {
            int num = 0;
            try
            {
                num = comando.ExecuteNonQuery(); //Ejecuta el comando y devuelve el resultado
            }
            catch (Exception ex)
            {
                throw new Exception("Error Ejecutando Acción - " + ex.Message, ex);
            }
            return num;
        }

        /// <summary>
        /// Metodo que devuelve un SqlDataReader enviandole como parametro un SqlCommand
        /// </summary>
        /// <param name="comando">Comando con la consulta que se quiere ejecutar</param>
        /// <returns>Devuelve un SqlDataReader</returns>
        public SqlDataReader Ejecuta_Consulta(SqlCommand comando)
        {
            SqlDataReader sqlDataReader;
            try
            {
                sqlDataReader = comando.ExecuteReader();
            }
            catch (Exception ex)
            {
                throw new Exception("Error Ejecutando consulta - " + ex.Message, ex);
            }
            finally
            {
                comando.Dispose();
            }
            return sqlDataReader;
        }

    }
}
