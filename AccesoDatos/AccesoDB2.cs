//-- ===============================================================================
//-- Author: Roberto D. Garcia
//-- Empresa : SYTCORP
//-- Create date: 11/06/2015
//-- Description: Genera metodos para facilitar el acceso a los datos en DB2.
//-- ================================================================================

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using IBM.Data;
using IBM.Data.DB2;
using IBM.Data.DB2Types;
namespace AccesoDatos
{
    public class AccesoDB2
    {
        //Variables para la conexion al servidor
        public string cConex;
        public DB2Connection conexion;

        public void AccesoDatos()
        {
            this.conexion = new DB2Connection(this.cConex);
        }

        /// <summary>
        /// Abre la conexion con la base de datos.
        /// </summary>
        public void AbrirConexion()
        {
            try
            {
                //Verifica el estado de la conexion
                if(this.conexion.State == System.Data.ConnectionState.Broken || this.conexion.State == System.Data.ConnectionState.Closed)
                {
                    this.conexion.Open(); //Si todo sale bien abre la conexion.
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error al conectarse al servidor de Base de Datos - " + ex.Message.ToString());
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
                if(this.conexion.State == System.Data.ConnectionState.Open)
                {
                    this.conexion.Close();
                }                
            }
            catch (Exception ex)
            {                
                throw new Exception("No se pudo cerrar la conexión - " + ex.Message.ToString());
            }
        }


        /// <summary>
        /// Crea un comando tipo StoredProcedure.
        /// </summary>
        /// <param name="vSP">Nombre del Stored Procedure.</param>
        /// <returns>Retorna un comando de tipo StoredProcedure.</returns>
        public DB2Command CrearComando(string vSP)
        {
            DB2Command db2Command = new DB2Command(vSP, this.conexion);
            db2Command.CommandType = CommandType.StoredProcedure;
            return db2Command;
        }

        /// <summary>
        /// Crea un comando del tipo Text
        /// </summary>
        /// <param name="query">Sentencia SQL</param>
        /// <returns>Retorna un comando de tipo text.</returns>
        public DB2Command CrearQuery(string query)
        {
            DB2Command db2Command = new DB2Command(query, this.conexion);
            db2Command.CommandType = CommandType.Text;
            return db2Command;
        }

        /// <summary>
        /// Metodo que devuelve un DataSet al enviar como parametro el comando SQL.
        /// </summary>
        /// <param name="vcmd">Comando SQL</param>
        /// <returns>Devuelve un DataSet.</returns>
        public DataSet GetDS(DB2Command vcmd)
        {
            DataSet dataset = new DataSet();
            try
            {
                new DB2DataAdapter(vcmd).Fill(dataset); //Se carga el query y llena el dataset que se retornara.
                return dataset;
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Error Devolviendo DataSet - {0}", ex.Message), ex);
            }
            finally
            {
                vcmd.Connection.Dispose();
                vcmd.Dispose();
            }
        }

        /// <summary>
        /// Ejecuta un accion mediante un DB2Command que se le envia como parametro.
        /// </summary>
        /// <param name="comando">Comando con la accion que se queria ejecutar</param>
        /// <returns>Retorna el numero de filas afectadas por la accion</returns>
        public int Ejecuta_Accion(ref DB2Command comando)
        {
            int num = 0;
            try
            {
                num = comando.ExecuteNonQuery(); //Ejecuta el comando y devuelve el resultado
                return num;
            }
            catch (Exception ex)
            {
                throw new Exception("Error Ejecutando Acción - " + ex.Message, ex);
            }
        }


        /// <summary>
        /// Metodo que devuelve un DB2DataReader enviandole como parametro un DB2Command
        /// </summary>
        /// <param name="comando"></param>
        /// <returns></returns>
        public DB2DataReader Ejecuta_Consulta(DB2Command comando)
        {
            DB2DataReader db2DataReader;
            try
            {
                db2DataReader = comando.ExecuteReader();
            }
            catch (Exception ex)
            {
                throw new Exception("Error Ejecutando consulta - " + ex.Message, ex);
            }
            finally 
            {
                comando.Dispose();
            }
            return db2DataReader;
        }

    }
}
