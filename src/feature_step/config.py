from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType, DecimalType

# Configuración de Neo4j
NEO4J_CONFIG = {
    "uri": "bolt://localhost:7687",
    "user": "neo4j",
    "password": "password"
}

# Configuración flexible de tablas
DATA_CONFIG = {
    "eventos": {
        "path": "s09.csv",
        "schema": StructType([
            StructField("data_date", TimestampType(), True),
            StructField("idrpt", StringType(), True),
            StructField("idpet", StringType(), True),
            StructField("version", StringType(), True),
            StructField("cnc_id", StringType(), True),
            StructField("cnt_id", StringType(), True),
            StructField("fh", TimestampType(), True),
            StructField("season", StringType(), True),
            StructField("et", LongType(), True),
            StructField("c", LongType(), True),
            StructField("d1", StringType(), True),
            StructField("d2", StringType(), True),
            StructField("zip_file", StringType(), True),
            StructField("xml_file", StringType(), True),
            StructField("partition_0", StringType(), True),
            StructField("partition_1", StringType(), True),
            StructField("partition_2", StringType(), True)
        ]),
        "date_columns": []
    },
    "curva_horaria": {
        "path": "s02.csv",
        "schema": StructType([
            StructField("data_date", TimestampType(), True),
            StructField("idrpt", StringType(), True),
            StructField("idpet", StringType(), True),
            StructField("version", StringType(), True),
            StructField("cnc_id", StringType(), True),
            StructField("cnt_id", StringType(), True),
            StructField("magn", LongType(), True),
            StructField("fh", TimestampType(), True),
            StructField("season", StringType(), True),
            StructField("bc", StringType(), True),
            StructField("ai", DoubleType(), True),
            StructField("ae", DoubleType(), True),
            StructField("r1", DoubleType(), True),
            StructField("r2", DoubleType(), True),
            StructField("r3", DoubleType(), True),
            StructField("r4", DoubleType(), True),
            StructField("zip_file", StringType(), True),
            StructField("xml_file", StringType(), True),
            StructField("aud_tim", TimestampType(), True),
            StructField("partition_0", StringType(), True),
            StructField("partition_1", StringType(), True),
            StructField("partition_2", StringType(), True)
        ]),
        "date_columns": []
    },
    "inspecciones": {
        "path": "ooss01.csv",
        "schema": StructType([
            StructField("csv_file", StringType(), True),
            StructField("data_date", StringType(), True),
            StructField("nis_rad", IntegerType(), True),
            StructField("estado_ctto", StringType(), True),
            StructField("fecha_alta_ctto", StringType(), True),
            StructField("fecha_alta_sum", StringType(), True),
            StructField("fecha_baja_ctto", StringType(), True),
            StructField("fecha_baja_ctto_ant", StringType(), True),
            StructField("solicitud", StringType(), True),
            StructField("proceso", StringType(), True),
            StructField("nif", StringType(), True),
            StructField("tarifa", StringType(), True),
            StructField("tip_os", StringType(), True),
            StructField("descripcion_os", StringType(), True),
            StructField("num_os", StringType(), True),
            StructField("cer", StringType(), True),
            StructField("fecha_generacion", StringType(), True),
            StructField("fuce", StringType(), True),
            StructField("acc_1", StringType(), True),
            StructField("acc_2", StringType(), True),
            StructField("acc_3", StringType(), True),
            StructField("acc_4", StringType(), True),
            StructField("acc_5", StringType(), True),
            StructField("acc_6", StringType(), True),
            StructField("acc_7", StringType(), True),
            StructField("acc_8", StringType(), True),
            StructField("acc_9", StringType(), True),
            StructField("acc_10", StringType(), True),
            StructField("acc_11", StringType(), True),
            StructField("acc_12", StringType(), True),
            StructField("acc_13", StringType(), True),
            StructField("acc_14", StringType(), True),
            StructField("acc_15", StringType(), True),
            StructField("precio_os", StringType(), True),
            StructField("num_apa1", StringType(), True),
            StructField("marca1", StringType(), True),
            StructField("tip_apa_1", StringType(), True),
            StructField("ten_apa_1", StringType(), True),
            StructField("f_inst_apa_1", StringType(), True),
            StructField("num_apa2", StringType(), True),
            StructField("marca2", StringType(), True),
            StructField("tip_apa_2", StringType(), True),
            StructField("ten_apa_2", StringType(), True),
            StructField("f_inst_apa_2", StringType(), True),
            StructField("num_apa3", StringType(), True),
            StructField("marca3", StringType(), True),
            StructField("tip_apa_3", StringType(), True),
            StructField("ten_apa_3", StringType(), True),
            StructField("f_inst_apa_3", StringType(), True),
            StructField("tipo_sum", StringType(), True),
            StructField("direccion", StringType(), True),
            StructField("ent_singular", StringType(), True),
            StructField("ent_colectiva", StringType(), True),
            StructField("municipio", StringType(), True),
            StructField("provincia", StringType(), True),
            StructField("area_ejecucion", StringType(), True),
            StructField("comentario_generacion", StringType(), True),
            StructField("descripcion_resolucion", StringType(), True),
            StructField("icp_instalado", StringType(), True),
            StructField("f_inst_icp", StringType(), True),
            StructField("fases", StringType(), True),
            StructField("intensidad_icp", StringType(), True),
            StructField("cod_resuelve_os", StringType(), True),
            StructField("cod_contrata", StringType(), True),
            StructField("contrata", StringType(), True),
            StructField("usuario_genera_os", StringType(), True),
            StructField("propiedad_apa", StringType(), True),
            StructField("nom_propiedad", StringType(), True),
            StructField("ult_lect", StringType(), True),
            StructField("fecha_ult_lect", StringType(), True),
            StructField("cnae", IntegerType(), True),
            StructField("tipo_punto", IntegerType(), True),
            StructField("fecha_primer_envio_contrata", StringType(), True),
            StructField("alimentacion", StringType(), True),
            StructField("coord_x", StringType(), True),
            StructField("coord_y", StringType(), True),
            StructField("per_lect", StringType(), True),
            StructField("cod_retorno_sat", IntegerType(), True),
            StructField("cod_lector", StringType(), True),
            StructField("nombre_lector", StringType(), True),
            StructField("cod_operario", StringType(), True),
            StructField("nombre_operario", StringType(), True),
            StructField("tip_apa_lvnto", StringType(), True),
            StructField("num_apa_lvnto", StringType(), True),
            StructField("marca_apa_lvnto", IntegerType(), True),
            StructField("lect_csmo_11", StringType(), True),
            StructField("lect_csmo_12", StringType(), True),
            StructField("lect_csmo_14", StringType(), True),
            StructField("estado_apa_inst_1", StringType(), True),
            StructField("estado_apa_inst_2", StringType(), True),
            StructField("estado_apa_inst_3", StringType(), True),
            StructField("cod_barras", StringType(), True),
            StructField("modelo", StringType(), True),
            StructField("amperios", StringType(), True),
            StructField("amperios_med1", StringType(), True),
            StructField("calle", StringType(), True),
            StructField("cbarras_med1", StringType(), True),
            StructField("cgv_pm", StringType(), True),
            StructField("clave_acometida", StringType(), True),
            StructField("cod_comer", StringType(), True),
            StructField("cod_postal", StringType(), True),
            StructField("dup", StringType(), True),
            StructField("fases_con", StringType(), True),
            StructField("fases_med1", StringType(), True),
            StructField("fecha_inst_con", StringType(), True),
            StructField("gcv", StringType(), True),
            StructField("int_prim_con", StringType(), True),
            StructField("lect_med11", StringType(), True),
            StructField("lect_med12", StringType(), True),
            StructField("lect_med13", StringType(), True),
            StructField("lect_med14", StringType(), True),
            StructField("lect_med15", StringType(), True),
            StructField("lect_med16", StringType(), True),
            StructField("lect_med17", StringType(), True),
            StructField("marca_con", StringType(), True),
            StructField("metid_med1", StringType(), True),
            StructField("nif_fi_apa", StringType(), True),
            StructField("nombre_usu", StringType(), True),
            StructField("nombre_usu_telef", StringType(), True),
            StructField("nombre_comer", StringType(), True),
            StructField("num_apa_con", StringType(), True),
            StructField("numero", StringType(), True),
            StructField("pot_ctto", StringType(), True),
            StructField("pot_max", StringType(), True),
            StructField("pot_pag_pr", StringType(), True),
            StructField("pot_hp6_1", StringType(), True),
            StructField("pot_hp6_2", StringType(), True),
            StructField("pot_hp6_3", StringType(), True),
            StructField("pot_hp6_4", StringType(), True),
            StructField("pot_hp6_5", StringType(), True),
            StructField("pot_hp6_6", StringType(), True),
            StructField("prop_con", StringType(), True),
            StructField("rel_trans_con", StringType(), True),
            StructField("tcsmo_med11", StringType(), True),
            StructField("tcsmo_med12", StringType(), True),
            StructField("tcsmo_med13", StringType(), True),
            StructField("tcsmo_med14", StringType(), True),
            StructField("tcsmo_med15", StringType(), True),
            StructField("tcsmo_med16", StringType(), True),
            StructField("tcsmo_med17", StringType(), True),
            StructField("tens_sum", StringType(), True),
            StructField("tension_con", StringType(), True),
            StructField("tip_apa_con", StringType(), True),
            StructField("tip_local", StringType(), True),
            StructField("tipo_via", StringType(), True),
            StructField("estado_os", StringType(), True),
            StructField("fecha_ini_os", StringType(), True),
            StructField("fecha_fin_os", StringType(), True),
            StructField("descripcion_acciones_os", StringType(), True),
            StructField("punto_medida", StringType(), True),
            StructField("motivo", StringType(), True),
            StructField("id_action1", StringType(), True),
            StructField("id_action2", StringType(), True),
            StructField("id_action3", StringType(), True),
            StructField("id_action4", StringType(), True),
            StructField("id_action5", StringType(), True),
            StructField("id_action6", StringType(), True),
            StructField("id_action7", StringType(), True),
            StructField("id_action8", StringType(), True),
            StructField("id_action9", StringType(), True),
            StructField("id_action10", StringType(), True),
            StructField("id_action11", StringType(), True),
            StructField("id_action12", StringType(), True),
            StructField("id_action13", StringType(), True),
            StructField("id_action14", StringType(), True),
            StructField("id_action15", StringType(), True),
            StructField("grupo_trabajo", StringType(), True),
            StructField("prioridad", StringType(), True),
            StructField("fecha_solicitud_ejecucion", StringType(), True),
            StructField("fecha_programada", StringType(), True),
            StructField("fecha_maxima_ejecucion", StringType(), True),
            StructField("fecha_minima_ejecucion", StringType(), True),
            StructField("aud_tim", TimestampType(), True),
            StructField("partition_0", StringType(), True),
            StructField("partition_1", StringType(), True),
            StructField("partition_2", StringType(), True)
        ]),
        "date_columns": [
            "data_date", "fecha_alta_sum", "fecha_alta_ctto", "fecha_baja_ctto", "fecha_baja_ctto_ant", "fuce",
            "fecha_generacion", "f_inst_apa_1", "f_inst_apa_2", "f_inst_apa_3", "f_inst_icp",
            "fecha_ult_lect", "fecha_primer_envio_contrata", "fecha_inst_con", "fecha_ini_os", "fecha_fin_os",
            "fecha_solicitud_ejecucion", "fecha_programada", "fecha_maxima_ejecucion", "fecha_minima_ejecucion"
        ],
        "date_format": "dd/MM/yyyy"
    },
    "grid_contadores": {
        "path": "grid_contadores.csv",
        "schema": StructType([
            StructField("_id", LongType(), True),
            StructField("data_date", StringType(), True),
            StructField("ct_sgc", StringType(), True),
            StructField("ct_gmo", StringType(), True),
            StructField("ct_bdi", StringType(), True),
            StructField("ct_odi", StringType(), True),
            StructField("nombre_ct_bdi", StringType(), True),
            StructField("cnc_s05", StringType(), True),
            StructField("trafo_bdi", IntegerType(), True),
            StructField("salida_bt_bdi", IntegerType(), True),
            StructField("clave_acometida_sgc", StringType(), True),
            StructField("clave_acometida_ficticia_sgc", StringType(), True),
            StructField("nif_sum_sgc", StringType(), True),
            StructField("cups_sgc", StringType(), True),
            StructField("nis_rad_sgc", IntegerType(), True),
            StructField("nif_pm_sgc", StringType(), True),
            StructField("cgv_pm_sgc", StringType(), True),
            StructField("cnt_sgc", StringType(), True),
            StructField("marca_cnt_sgc", StringType(), True),
            StructField("mod_s06", StringType(), True),
            StructField("codigo_barras_s06", StringType(), True),
            StructField("fecha_ult_s06_s06", StringType(), True),
            StructField("vprime_s06", StringType(), True),
            StructField("vf_s06", StringType(), True),
            StructField("version_stg_s05", StringType(), True),
            StructField("companion_s06", StringType(), True),
            StructField("ultimo_domingo", StringType(), True),
            StructField("fecha_ult_s05_s05", StringType(), True),
            StructField("num_dias_uls_s05_s05", IntegerType(), True),
            StructField("num_dias_ult_s05_desde_domingo_s05", IntegerType(), True),
            StructField("num_dias_ult_s05_o_inst_desde_domingo_s05", IntegerType(), True),
            StructField("aea_s05", IntegerType(), True),
            StructField("aia_s05", IntegerType(), True),
            StructField("num_envios_s05", IntegerType(), True),
            StructField("num_envios_s02_s02", IntegerType(), True),
            StructField("fecha_ult_s02_s02", StringType(), True),
            StructField("num_horas_s02_s02", IntegerType(), True),
            StructField("num_huecos_s02_s02", IntegerType(), True),
            StructField("num_dias_ult_s02_s02", IntegerType(), True),
            StructField("num_dias_ult_s02_desde_domingo_s02", IntegerType(), True),
            StructField("num_dias_ult_s02_o_inst_desde_domingo_s02", IntegerType(), True),
            StructField("fecha_inst_apa_sgc", StringType(), True),
            StructField("num_dias_inst_apa_sgc", IntegerType(), True),
            StructField("inst_tg_sgc", StringType(), True),
            StructField("telegest_os_sgc", StringType(), True),
            StructField("fr_sgc", StringType(), True),
            StructField("primera_fr_zeus", StringType(), True),
            StructField("aor_fin_pm_sgc", StringType(), True),
            StructField("f_puesta_explotacion_sgc", StringType(), True),
            StructField("tipo_punto_sgc", IntegerType(), True),
            StructField("tipo_sum_sgc", StringType(), True),
            StructField("estado_contrato_sgc", StringType(), True),
            StructField("fases_sgc", StringType(), True),
            StructField("prop_apa_sgc", StringType(), True),
            StructField("alquiler_distribuidora_zeus", StringType(), True),
            StructField("pot_ctto_sgc", IntegerType(), True),
            StructField("pot_max_sgc", IntegerType(), True),
            StructField("tension_sum_sgc", StringType(), True),
            StructField("comercializadora_sgc", StringType(), True),
            StructField("tarifa_sgc", StringType(), True),
            StructField("alquiler_sgc", StringType(), True),
            StructField("fecha_alta_ctto_sgc", StringType(), True),
            StructField("fecha_baja_ctto_sgc", StringType(), True),
            StructField("complemento_fact_sgc", StringType(), True),
            StructField("fecha_ult_lect_sgc", StringType(), True),
            StructField("fecha_ult_fact_sgc", StringType(), True),
            StructField("num_dias_ult_fact_sgc", IntegerType(), True),
            StructField("contrata_tg_contratas", StringType(), True),
            StructField("contrata_zona_contratas", StringType(), True),
            StructField("area_ejecucion_sgc", StringType(), True),
            StructField("centro_tecnico_sgc", StringType(), True),
            StructField("coord_utm_x_bdi", StringType(), True),
            StructField("coord_utm_y_bdi", StringType(), True),
            StructField("meterid_1_sgc", StringType(), True),
            StructField("meterid_2_sgc", StringType(), True),
            StructField("meterid_3_sgc", StringType(), True),
            StructField("tipo_apa1_sgc", StringType(), True),
            StructField("marca1_sgc", StringType(), True),
            StructField("num_apa1_sgc", StringType(), True),
            StructField("mod1_sgc", StringType(), True),
            StructField("cod_barras1_sgc", StringType(), True),
            StructField("subtipo_apa1_zeus", StringType(), True),
            StructField("tipo_apa2_sgc", StringType(), True),
            StructField("marca2_sgc", StringType(), True),
            StructField("num_apa2_sgc", StringType(), True),
            StructField("mod2_sgc", StringType(), True),
            StructField("cod_barras2_sgc", StringType(), True),
            StructField("subtipo_apa2_zeus", StringType(), True),
            StructField("tipo_apa3_sgc", StringType(), True),
            StructField("marca3_sgc", StringType(), True),
            StructField("num_apa3_sgc", StringType(), True),
            StructField("mod3_sgc", StringType(), True),
            StructField("cod_barras3_sgc", StringType(), True),
            StructField("reactiva_zeus", StringType(), True),
            StructField("maximetro_icp_zeus", StringType(), True),
            StructField("cgv_sum_sgc", StringType(), True),
            StructField("tipo_via_sgc", StringType(), True),
            StructField("calle_sgc", StringType(), True),
            StructField("numero_sgc", StringType(), True),
            StructField("duplicador_sgc", StringType(), True),
            StructField("cod_postal_sgc", StringType(), True),
            StructField("entidad_singular_sgc", StringType(), True),
            StructField("entidad_colectiva_sgc", StringType(), True),
            StructField("municipio_sgc", StringType(), True),
            StructField("provincia_sgc", StringType(), True),
            StructField("ccaa_sgc", StringType(), True),
            StructField("trafos_en_ct", IntegerType(), True),
            StructField("salidas_bt_en_ct", IntegerType(), True),
            StructField("salidas_bt_en_trafo", IntegerType(), True),
            StructField("acometidas_en_ct", IntegerType(), True),
            StructField("acometidas_en_trafo", IntegerType(), True),
            StructField("acometidas_por_salida_bt", IntegerType(), True),
            StructField("contadores_tg_en_ct", IntegerType(), True),
            StructField("contadores_tg_en_trafo", IntegerType(), True),
            StructField("contadores_tg_en_salida_bt", IntegerType(), True),
            StructField("contadores_tg_en_acometida", IntegerType(), True),
            StructField("tasa_comunica_ct_s05", DecimalType(16, 13), True),
            StructField("tasa_comunica_trafo_s05", DecimalType(16, 13), True),
            StructField("tasa_comunica_salida_bt_s05", DecimalType(16, 13), True),
            StructField("tasa_comunica_acometida_s05", DecimalType(16, 13), True),
            StructField("tasa_5_7_ct_s05", DecimalType(16, 13), True),
            StructField("tasa_5_7_trafo_s05", DecimalType(16, 13), True),
            StructField("tasa_5_7_salida_bt_s05", DecimalType(16, 13), True),
            StructField("tasa_5_7_acometida_s05", DecimalType(16, 13), True),
            StructField("num_trafos_comunica_ko_por_ct", IntegerType(), True),
            StructField("num_trafos_comunica_parcial_por_ct", IntegerType(), True),
            StructField("num_trafos_comunica_ok_por_ct", IntegerType(), True),
            StructField("num_lineas_comunica_ko_por_trafo", IntegerType(), True),
            StructField("num_lineas_comunica_parcial_por_trafo", IntegerType(), True),
            StructField("num_lineas_comunica_ok_por_trafo", IntegerType(), True),
            StructField("num_acometidas_comunica_ko_por_linea", IntegerType(), True),
            StructField("num_acometidas_comunica_parcial_por_linea", IntegerType(), True),
            StructField("num_acometidas_comunica_ok_por_linea", IntegerType(), True),
            StructField("ultimo_s05_distinto_0_odi", StringType(), True),
            StructField("ultimo_s02_distinto_0_odi", StringType(), True),
            StructField("estado_tg_gct", StringType(), True),
            StructField("num_os_ult_inc_sgc", StringType(), True),
            StructField("tipo_os_ult_inc_sgc", StringType(), True),
            StructField("cer_ult_inc_sgc", StringType(), True),
            StructField("fuce_ult_inc_sgc", StringType(), True),
            StructField("num_dias_os_ult_inc_sgc", IntegerType(), True),
            StructField("accs_ult_inc_sgc", StringType(), True),
            StructField("com_resolucion_ult_inc_zeus", StringType(), True),
            StructField("num_os_ult_sgc", StringType(), True),
            StructField("tipo_os_ult_sgc", StringType(), True),
            StructField("cer_ult_sgc", StringType(), True),
            StructField("fuce_ult_sgc", StringType(), True),
            StructField("num_dias_os_ult_sgc", IntegerType(), True),
            StructField("accs_ult_sgc", StringType(), True),
            StructField("com_resolucion_ult_zeus", StringType(), True),
            StructField("num_os_ult_acom_zeus", StringType(), True),
            StructField("tipo_os_ult_acom_zeus", StringType(), True),
            StructField("cer_ult_acom_zeus", StringType(), True),
            StructField("fuce_ult_acom_zeus", StringType(), True),
            StructField("num_dias_os_ult_acom_zeus", IntegerType(), True),
            StructField("accs_ult_acom_zeus", StringType(), True),
            StructField("com_resolucion_ult_acom_zeus", StringType(), True),
            StructField("favoritos_edit", StringType(), True),
            StructField("contrasenyas_edit", StringType(), True),
            StructField("criticidad_edit", StringType(), True),
            StructField("num_incidencia", StringType(), True),
            StructField("estado_incidencia", StringType(), True),
            StructField("fecha_estado_incidencia", StringType(), True),
            StructField("sintoma_incidencia", StringType(), True),
            StructField("reglas_cumplidas", StringType(), True),
            StructField("estado_incidencia_concentrador", StringType(), True),
            StructField("num_dias_ult_cambio_inc_concentrador", IntegerType(), True),
            StructField("afectado_por_incidencia_cargas", StringType(), True),
            StructField("afectado_por_incidencia_concentrador", StringType(), True),
            StructField("afectado_por_incidencia_ruido", StringType(), True),
            StructField("nivel_ruido", StringType(), True),
            StructField("num_incidencia_ruido", StringType(), True),
            StructField("afectado_por_incidencia_pvpc", StringType(), True),
            StructField("g02_semanal", StringType(), True),
            StructField("t1_modelo_g17", StringType(), True),
            StructField("t1_version_fw_g17", StringType(), True),
            StructField("afectado_por_estrategia", StringType(), True),
            StructField("_0_mk_pkms_6b", StringType(), True),
            StructField("_1_lls_pkms_6b", StringType(), True),
            StructField("_2_ak_pkms_6b", StringType(), True),
            StructField("_4_uk_pkms_6b", StringType(), True),
            StructField("_5_factory_lls_pkms_6b", StringType(), True),
            StructField("_6_factory_mk_pkms_6b", StringType(), True),
            StructField("creation_date_mk_pkms_6b", StringType(), True),
            StructField("creation_date_lls_pkms_6b", StringType(), True),
            StructField("creation_date_ak_pkms_6b", StringType(), True),
            StructField("creation_date_uk_pkms_6b", StringType(), True),
            StructField("creation_date_factory_lls_pkms_6b", StringType(), True),
            StructField("creation_date_factory_mk_pkms_6b", StringType(), True),
            StructField("security_status_security", StringType(), True),
            StructField("dc_changed_security", StringType(), True),
            StructField("meter_dc_status_security", StringType(), True),
            StructField("activate_key_status_security", StringType(), True),
            StructField("master_key_status_security", StringType(), True),
            StructField("authenticate_key_status_security", StringType(), True),
            StructField("unicast_key_status_security", StringType(), True),
            StructField("master_activation_key_security", StringType(), True),
            StructField("global_activation_key_security", StringType(), True),
            StructField("fecha_ultimo_s24", StringType(), True),
            StructField("fecha_ultimo_acceso_s24", StringType(), True),
            StructField("ultimo_estado_s24", StringType(), True),
            StructField("active_s24", StringType(), True),
            StructField("fecha_ultimo_s31", StringType(), True),
            StructField("s31_estado_cliente_dc", StringType(), True),
            StructField("s31_mascara_de_claves", StringType(), True),
            StructField("s31_cliente_id", StringType(), True),
            StructField("fecha_ultimo_s32", StringType(), True),
            StructField("origen", StringType(), True),
            StructField("cnae", StringType(), True),
            StructField("tarifa_activa_s23", StringType(), True),
            StructField("tarifa_latente_s23", StringType(), True),
            StructField("fecha_activacion_contrato_latente_s23", StringType(), True),
            StructField("tr1_s23", StringType(), True),
            StructField("tr2_s23", StringType(), True),
            StructField("tr3_s23", StringType(), True),
            StructField("fecha_ultimo_s23_s23", StringType(), True),
            StructField("ruta", StringType(), True),
            StructField("fecha_ult_s19_s19", StringType(), True),
            StructField("vf_s19", StringType(), True),
            StructField("vprime_s19", StringType(), True),
            StructField("vf_s06_s19", StringType(), True),
            StructField("vprime_s06_s19", StringType(), True),
            StructField("fecha_activacion_contrato_activo_s23", StringType(), True),
            StructField("lote_manual", StringType(), True),
            StructField("lote_telegestionado", StringType(), True),
            StructField("consumo_facturado_ultimo_anyo", StringType(), True),
            StructField("consumo_facturado_el_penultimo_anyo", StringType(), True),
            StructField("ultimo_consumo_facturado", StringType(), True),
            StructField("estado_incidencia_ruido", StringType(), True),
            StructField("filtros_plc_numero_zeus", StringType(), True),
            StructField("filtros_plc_marca_mod_zeus", StringType(), True),
            StructField("filtros_plc_fecha_1__inst_zeus", StringType(), True),
            StructField("filtros_plc_fecha_ult_inst_zeus", StringType(), True),
            StructField("filtros_plc_iguales_zeus", StringType(), True),
            StructField("hora_activacion_contrato_latente_s23", StringType(), True),
            StructField("hora_activacion_contrato_activo_s23", StringType(), True),
            StructField("tarifa_empleado", StringType(), True),
            StructField("suministro_no_cortable", StringType(), True),
            StructField("suministro_programable", StringType(), True),
            StructField("pot_contratada_p1", StringType(), True),
            StructField("max_s04", StringType(), True),
            StructField("fx_s04", StringType(), True),
            StructField("fecha_ult_s04_s04", StringType(), True),
            StructField("fecha_fabricacion_s06", StringType(), True),
            StructField("ciclos_estimados_zeus", IntegerType(), True),
            StructField("cnt_gmo", StringType(), True),
            StructField("cnt_online_gmo", StringType(), True),
            StructField("filepath", StringType(), True),
            StructField("data_date_ing", LongType(), True),
            StructField("op", StringType(), True)
        ]),
        "date_columns": ["data_date"],
        "date_format": "dd/MM/yyyy H:mm"
    },
    "denuncias": {
        "path": "denuncias.csv",
        "schema": StructType([
            StructField("n__denuncia", StringType(), True),
            StructField("cups", StringType(), True),
            StructField("fec_alta", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("filepath", StringType(), True),
            StructField("data_date", LongType(), True),
            StructField("op", StringType(), True)
        ]),
        "date_columns": ["data_date"],
        "date_format": "dd/MM/yyyy"
    },
    "expedientes": {
        "path": "expedientes.csv",
        "schema": StructType([
            StructField("n__informe", StringType(), True),
            StructField("dg", IntegerType(), True),
            StructField("cups", StringType(), True),
            StructField("nis", StringType(), True),
            StructField("fecha_inicio_anomalia", StringType(), True),
            StructField("fecha_fin_anomalia", StringType(), True),
            StructField("tipo_anomalia", StringType(), True),
            StructField("irregularidades_detectadas", StringType(), True),
            StructField("clasificacion", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("energia_facturada", IntegerType(), True),
            StructField("valoracion_total", IntegerType(), True),
            StructField("num_factura_atr", StringType(), True),
            StructField("num_ooss", StringType(), True),
            StructField("fec_estado", StringType(), True),
            StructField("fec_alta", StringType(), True),
            StructField("num_factura_levantamiento", StringType(), True),
            StructField("fechainiciofacturaemitida", StringType(), True),
            StructField("fechafinfacturaemitida", StringType(), True),
            StructField("tarifa", StringType(), True),
            StructField("metodo_estimacion", StringType(), True),
            StructField("estado_suministro", StringType(), True),
            StructField("motivonofacturable", StringType(), True),
            StructField("fec_acta", StringType(), True),
            StructField("dias_liquidables", IntegerType(), True),
            StructField("liquidable", DoubleType(), True),
            StructField("energia_liquidable", IntegerType(), True),
            StructField("codigo_comercializadora", StringType(), True),
            StructField("nombre_comercializadora", StringType(), True),
            StructField("filepath", StringType(), True),
            StructField("data_date", LongType(), True),
            StructField("op", StringType(), True)
        ]),
        "date_columns": ["data_date"],
        "date_format": "dd/MM/yyyy"
    },
    "grupo_eventos_codigos": {
        "path": "grupo_eventos_codigos.csv",
        "schema": StructType([
            StructField("grupo", StringType(), True),
            StructField("subgrupo", StringType(), True),
            StructField("descripcion", StringType(), True),
            StructField("numero", StringType(), True),
            StructField("desc_evento", StringType(), True)
        ]),
        "date_columns": []
    }
}


# ===================================================
# CONFIGURACIONES PARA NEO4J
# ===================================================

# Configuración para Inspecciones
INSPECCIONES_CONFIG = {
    'nodo_label': 'Inspecciones',
    'campos_constraint': ['nis_rad', 'num_os'],
    'campos_indices': ['nis_rad', 'num_os'],
    'todos_los_campos': [
        'csv_file', 'data_date', 'nis_rad', 'estado_ctto', 'fecha_alta_ctto', 'fecha_alta_sum',
        'fecha_baja_ctto', 'fecha_baja_ctto_ant', 'solicitud', 'proceso', 'nif', 'tarifa', 'tip_os',
        'descripcion_os', 'num_os', 'cer', 'fecha_generacion', 'fuce', 'acc_1', 'acc_2', 'acc_3', 'acc_4',
        'acc_5', 'acc_6', 'acc_7', 'acc_8', 'acc_9', 'acc_10', 'acc_11', 'acc_12', 'acc_13', 'acc_14', 'acc_15',
        'precio_os', 'num_apa1', 'marca1', 'tip_apa_1', 'ten_apa_1', 'f_inst_apa_1', 'num_apa2', 'marca2',
        'tip_apa_2', 'ten_apa_2', 'f_inst_apa_2', 'num_apa3', 'marca3', 'tip_apa_3', 'ten_apa_3', 'f_inst_apa_3',
        'tipo_sum', 'direccion', 'ent_singular', 'ent_colectiva', 'municipio', 'provincia', 'area_ejecucion',
        'comentario_generacion', 'descripcion_resolucion', 'icp_instalado', 'f_inst_icp', 'fases', 'intensidad_icp',
        'cod_resuelve_os', 'cod_contrata', 'contrata', 'usuario_genera_os', 'propiedad_apa', 'nom_propiedad',
        'ult_lect', 'fecha_ult_lect', 'cnae', 'tipo_punto', 'fecha_primer_envio_contrata', 'alimentacion',
        'coord_x', 'coord_y', 'per_lect', 'cod_retorno_sat', 'cod_lector', 'nombre_lector', 'cod_operario',
        'nombre_operario', 'tip_apa_lvnto', 'num_apa_lvnto', 'marca_apa_lvnto', 'lect_csmo_11', 'lect_csmo_12',
        'lect_csmo_14', 'estado_apa_inst_1', 'estado_apa_inst_2', 'estado_apa_inst_3', 'cod_barras', 'modelo',
        'amperios', 'amperios_med1', 'calle', 'cbarras_med1', 'cgv_pm', 'clave_acometida', 'cod_comer', 'cod_postal',
        'dup', 'fases_con', 'fases_med1', 'fecha_inst_con', 'gcv', 'int_prim_con', 'lect_med11', 'lect_med12',
        'lect_med13', 'lect_med14', 'lect_med15', 'lect_med16', 'lect_med17', 'marca_con', 'metid_med1', 'nif_fi_apa',
        'nombre_usu', 'nombre_usu_telef', 'nombre_comer', 'num_apa_con', 'numero', 'pot_ctto', 'pot_max', 'pot_pag_pr',
        'pot_hp6_1', 'pot_hp6_2', 'pot_hp6_3', 'pot_hp6_4', 'pot_hp6_5', 'pot_hp6_6', 'prop_con', 'rel_trans_con',
        'tcsmo_med11', 'tcsmo_med12', 'tcsmo_med13', 'tcsmo_med14', 'tcsmo_med15', 'tcsmo_med16', 'tcsmo_med17',
        'tens_sum', 'tension_con', 'tip_apa_con', 'tip_local', 'tipo_via', 'estado_os', 'fecha_ini_os', 'fecha_fin_os',
        'descripcion_acciones_os', 'punto_medida', 'motivo', 'id_action1', 'id_action2', 'id_action3', 'id_action4',
        'id_action5', 'id_action6', 'id_action7', 'id_action8', 'id_action9', 'id_action10', 'id_action11', 'id_action12',
        'id_action13', 'id_action14', 'id_action15', 'grupo_trabajo', 'prioridad', 'fecha_solicitud_ejecucion',
        'fecha_programada', 'fecha_maxima_ejecucion', 'fecha_minima_ejecucion', 'aud_tim', 'partition_0', 'partition_1', 'partition_2'
    ]
}

# Configuración para Curva Horaria
CURVA_HORARIA_CONFIG = {
    'nodo_label': 'Curva_horaria',
    'campos_constraint': ['data_date', 'idrpt', 'idpet', 'cnc_id', 'cnt_id'],
    'campos_indices': ['data_date', 'cnc_id', 'cnt_id', 'season', 'idrpt', 'idpet'],
    'todos_los_campos': [
        'data_date', 'idrpt', 'idpet', 'version', 'cnc_id', 'cnt_id', 'magn', 
        'fh', 'season', 'bc', 'ai', 'ae', 'r1', 'r2', 'r3', 'r4', 'zip_file', 
        'xml_file', 'aud_tim', 'partition_0', 'partition_1', 'partition_2'
    ]
}

# Configuración para Eventos
EVENTOS_CONFIG = {
    'nodo_label': 'Evento',
    'campos_constraint': ['data_date', 'idrpt', 'idpet', 'cnc_id', 'cnt_id'],
    'campos_indices': ['data_date', 'cnc_id', 'cnt_id', 'season', 'idrpt', 'idpet'],
    'todos_los_campos': [
        'data_date', 'idrpt', 'idpet', 'version', 'cnc_id', 'cnt_id', 'fh', 'season', 
        'et', 'c', 'd1', 'd2', 'zip_file', 'xml_file', 'partition_0', 'partition_1', 'partition_2'
    ]
}

# Configuración para Grid Contadores
GRID_CONTADORES_CONFIG = {
    'nodo_label': 'Grid_contadores',
    'campos_constraint': ['data_date', 'nis_rad_sgc'],  
    'campos_indices': [
        'data_date', 'nis_rad_sgc', 'cups_sgc', 'ct_sgc', 'cnc_s05', 
        'cnt_sgc', 'meterid_1_sgc', 'clave_acometida_sgc', 'estado_contrato_sgc',
        'municipio_sgc', 'provincia_sgc', 'ccaa_sgc'
    ],  
    'todos_los_campos': [
        '_id', 'data_date', 'ct_sgc', 'ct_gmo', 'ct_bdi', 'ct_odi', 'nombre_ct_bdi', 'cnc_s05', 
        'trafo_bdi', 'salida_bt_bdi', 'clave_acometida_sgc', 'clave_acometida_ficticia_sgc', 
        'nif_sum_sgc', 'cups_sgc', 'nis_rad_sgc', 'nif_pm_sgc', 'cgv_pm_sgc', 'cnt_sgc', 
        'marca_cnt_sgc', 'filepath', 'data_date_ing', 'op'
        # Nota: Schema truncado para brevedad - en producción incluir todos los campos
    ]
}

# Configuración para Denuncias
DENUNCIAS_CONFIG = {
    'nodo_label': 'Denuncias',
    'campos_constraint': ['data_date', 'n__denuncia', 'cups'],
    'campos_indices': ['data_date', 'n__denuncia', 'cups', 'estado', 'fec_alta'],
    'todos_los_campos': [
        'n__denuncia', 'cups', 'fec_alta', 'estado', 'filepath', 'data_date', 'op'
    ]
}

# Configuración para Expedientes
EXPEDIENTES_CONFIG = {
    'nodo_label': 'Expedientes',
    'campos_constraint': ['data_date', 'n__informe', 'cups', 'nis'],
    'campos_indices': ['data_date', 'n__informe', 'cups', 'nis', 'estado', 'tipo_anomalia', 'clasificacion', 'fec_alta'],
    'todos_los_campos': [
        'n__informe', 'dg', 'cups', 'nis', 'fecha_inicio_anomalia', 'fecha_fin_anomalia', 
        'tipo_anomalia', 'irregularidades_detectadas', 'clasificacion', 'estado', 
        'energia_facturada', 'valoracion_total', 'num_factura_atr', 'num_ooss', 'fec_estado', 'fec_alta'
        'num_factura_levantamiento', 'fechainiciofacturaemitida', 'fechafinfacturaemitida', 'tarifa', 'metodo_estimacion',
        'estado_suministro', 'motivonofacturable', 'fec_acta', 'dias_liquidables', 'liquidable',
        'energia_liquidable', 'codigo_comercializadora', 'nombre_comercializadora',
        'filepath', 'data_date', 'op'
    ]
}

# Configuración para Grupos códigos eventos
GRUPO_EVENTOS_CODIGOS_CONFIG = {
    'nodo_label': 'Grupo_eventos_codigos',
    'campos_constraint': ['grupo', 'subgrupo', 'numero'],
    'campos_indices': ['grupo', 'subgrupo', 'descripcion', 'numero'],
    'todos_los_campos': [
        'grupo', 'subgrupo', 'descripcion', 'numero', 'desc_evento'
    ]
}

# ===================================================
# CONFIGURACIONES PARA RELACIONES NEO4J
# ===================================================

# Configuración de relaciones entre nodos
RELACIONES_CONFIG = [
    {
        "nombre": "INSPECCION_EN_GRID",
        "descripcion": "Relaciona inspecciones con grid de contadores por NIS",
        "query": """
        MATCH (i:Inspecciones), (g:Grid_contadores)
        WHERE i.nis_rad IS NOT NULL 
        AND g.nis_rad_sgc IS NOT NULL
        AND i.nis_rad = g.nis_rad_sgc
        CREATE (i)-[:PERTENECE_A_GRID {
            created_at: datetime(),
            tipo_relacion: 'por_nis'
        }]->(g)
        """
    },
    {
        "nombre": "EVENTO_EN_CONTADOR",
        "descripcion": "Relaciona eventos con contadores por CNC_ID",
        "query": """
        MATCH (e:Evento), (g:Grid_contadores)
        WHERE e.cnc_id IS NOT NULL 
        AND g.cnc_s05 IS NOT NULL
        AND e.cnc_id = g.cnc_s05
        CREATE (e)-[:OCURRE_EN_CONTADOR {
            created_at: datetime(),
            tipo_relacion: 'por_cnc'
        }]->(g)
        """
    },
    {
        "nombre": "CURVA_EN_CONTADOR",
        "descripcion": "Relaciona curva horaria con contadores por CNC_ID",
        "query": """
        MATCH (c:Curva_horaria), (g:Grid_contadores)
        WHERE c.cnc_id IS NOT NULL 
        AND g.cnc_s05 IS NOT NULL
        AND c.cnc_id = g.cnc_s05
        CREATE (c)-[:MEDICION_EN_CONTADOR {
            created_at: datetime(),
            tipo_relacion: 'por_cnc'
        }]->(g)
        """
    },
    {
        "nombre": "DENUNCIA_EN_CUPS",
        "descripcion": "Relaciona denuncias con grid por CUPS",
        "query": """
        MATCH (d:Denuncias), (g:Grid_contadores)
        WHERE d.cups IS NOT NULL 
        AND g.cups_sgc IS NOT NULL
        AND d.cups = g.cups_sgc
        CREATE (d)-[:AFECTA_A_SUMINISTRO {
            created_at: datetime(),
            tipo_relacion: 'por_cups'
        }]->(g)
        """
    },
    {
        "nombre": "EXPEDIENTE_EN_CUPS",
        "descripcion": "Relaciona expedientes con grid por CUPS",
        "query": """
        MATCH (exp:Expedientes), (g:Grid_contadores)
        WHERE exp.cups IS NOT NULL 
        AND g.cups_sgc IS NOT NULL
        AND exp.cups = g.cups_sgc
        CREATE (exp)-[:INVESTIGACION_EN_SUMINISTRO {
            created_at: datetime(),
            tipo_relacion: 'por_cups'
        }]->(g)
        """
    },
    {
        "nombre": "EVENTO_CON_CODIGO",
        "descripcion": "Relaciona eventos con sus códigos descriptivos",
        "query": """
        MATCH (e:Evento), (gc:Grupo_eventos_codigos)
        WHERE e.et IS NOT NULL 
        AND gc.numero IS NOT NULL
        AND toString(e.et) = gc.numero
        CREATE (e)-[:TIPO_EVENTO {
            created_at: datetime(),
            tipo_relacion: 'por_codigo_evento'
        }]->(gc)
        """
    },
    {
        "nombre": "INSPECCION_MISMA_FECHA",
        "descripcion": "Relaciona inspecciones que ocurrieron el mismo día",
        "query": """
        MATCH (i1:Inspecciones), (i2:Inspecciones)
        WHERE i1.data_date IS NOT NULL 
        AND i2.data_date IS NOT NULL
        AND i1.data_date = i2.data_date
        AND i1.num_os <> i2.num_os
        AND i1.nis_rad <> i2.nis_rad
        CREATE (i1)-[:INSPECCION_SIMULTANEA {
            created_at: datetime(),
            fecha_inspeccion: i1.data_date,
            tipo_relacion: 'misma_fecha'
        }]->(i2)
        """
    },
    {
        "nombre": "EVENTO_CONSECUTIVO",
        "descripcion": "Relaciona eventos consecutivos en el mismo contador",
        "query": """
        MATCH (e1:Evento), (e2:Evento)
        WHERE e1.cnc_id = e2.cnc_id
        AND e1.cnt_id = e2.cnt_id
        AND e1.fh < e2.fh
        AND duration.between(e1.fh, e2.fh).hours <= 24
        WITH e1, e2, duration.between(e1.fh, e2.fh) as duracion
        CREATE (e1)-[:EVENTO_SIGUIENTE {
            created_at: datetime(),
            duracion_horas: duracion.hours,
            tipo_relacion: 'consecutivo'
        }]->(e2)
        """
    }
]

# Configuración de tablas para Neo4j
tablas_neo4j = {
    "denuncias": {
        "dataframe": "df_denuncias",
        "nombre_neo4j": "Denuncias",
        "config": "DENUNCIAS_CONFIG"
    },
    "eventos": {
        "dataframe": "df_eventos", 
        "nombre_neo4j": "Evento",
        "config": "EVENTOS_CONFIG"
    },
    "expedientes": {
        "dataframe": "df_expedientes",
        "nombre_neo4j": "Expedientes", 
        "config": "EXPEDIENTES_CONFIG"
    },
    "inspecciones": {
        "dataframe": "df_inspecciones",
        "nombre_neo4j": "Inspecciones",
        "config": "INSPECCIONES_CONFIG"
    },
    "grid_contadores": {
        "dataframe": "df_grid_contadores",
        "nombre_neo4j": "Grid_contadores", 
        "config": "GRID_CONTADORES_CONFIG"
    },
    "grupo_eventos_codigos": {
        "dataframe": "df_grupo_eventos_codigos",
        "nombre_neo4j": "Grupo_eventos_codigos",
        "config": "GRUPO_EVENTOS_CODIGOS_CONFIG"
    },
    "curva_horaria": {
        "dataframe": "df_curva_horaria",
        "nombre_neo4j": "Curva_horaria",
        "config": "CURVA_HORARIA_CONFIG"
    }
}

# Configuración para validaciones previas
VALIDACIONES_RELACIONES = {
    "verificar_nodos_antes_relaciones": True,
    "mostrar_estadisticas_previas": True,
    "batch_size_relaciones": 1000,
    "timeout_relaciones": 300  # segundos
}

