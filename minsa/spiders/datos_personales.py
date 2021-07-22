# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
import json
from minsa.items import PersonaItem
from datetime import datetime

class DatosPersonalesSpider(scrapy.Spider):
    name = 'datos_personales'
    YYYYMMDD_HHMMSS = datetime.now().strftime("%Y%m%d_%H%M%S")

    def __init__(self, *args, **kwargs):
        super(DatosPersonalesSpider, self).__init__(*args, **kwargs)
        self.in_path = './1_INPUT/'
        self.out_path = './2_OUTPUT/'
        self.main_URL = 'http://prosalud.minsa.gob.pe/titulo/titulo/persona'

        # INPUT
        dtype = {'DNI': str, 'APELLIDO': str}
        self.input_df = pd.read_csv(self.in_path+self.filename, sep='\t', usecols=['DNI','APELLIDO'], dtype=dtype, encoding='utf8')

        # CONTROL
        self.filepath_log = f"{self.out_path}datos_personales_{self.filename.split('.')[0]}_log_{self.YYYYMMDD_HHMMSS}.txt"
        with open(self.filepath_log, 'wb') as file:
            file.write('dni\tapellido\tfecha_consulta\testado\tmensaje\n'.encode("utf8"))

    def start_requests(self):
        #self.logger.info('='*30)
        #start = datetime.now()
        #self.logger.info('Función start_requests')
        
        for row_idx, row in self.input_df.iterrows():
            dni = row['DNI']
            apellido = row['APELLIDO']
            #self.logger.info(f'DNI={dni}, APELLIDO={apellido}')
            
            # Enviamos el POST
            formdata = {
                'C': 'PERSONA',
                'S': 'GETBYID',
                'numdoc': dni,
                'apelpaterno': apellido,
                'idtipodoc': '1'
            }
            meta = {
                'dni': dni,
                'apellido': apellido,
                'cookiejar': row_idx + 1
            }
            headers = {
                "Content-Type": " application/x-www-form-urlencoded; charset=UTF-8"
            }
            yield scrapy.FormRequest(self.main_URL, callback=self.parse, formdata=formdata, meta=meta, headers=headers)

    def parse(self, response):
        #self.logger.info('='*30)
        #self.logger.info('Función parse')    
        item = PersonaItem()
        response_data = json.loads(response.text)
        
        dni = response.meta.get('dni')
        apellido = response.meta.get('apellido')

        fecha_consulta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f'{dni}\t{apellido}\t{fecha_consulta}\t'
        if 'elsedelif' in response_data:
            elsedelif = response_data['elsedelif']
            active = response_data['active']
            line += f'ERROR\tNo existe el DNI.\n'
            #msj = 'No existe el DNI'
            #self.logger.error(f'Problema con el DNI {dni}: No existe (elsedelif={elsedelif}, active={active}).')
        elif 'error' in response_data:
            error = response_data['error']
            line += f'ERROR\tNo coincide el apellido.\n'
            #msj = 'No coincide el apellido'
            #self.logger.error(f'Problema con el DNI {dni} y apellido {apellido}: {error}.')
        else:
            item['fgreniec'] = response_data['fgreniec']
            item['descresubi'] = response_data['descresubi']
            item['apelmatpac'] = response_data['apelmatpac']
            item['direccion'] = response_data['direccion']
            item['idsexo'] = response_data['idsexo']
            item['nombpac'] = response_data['nombpac']
            item['numdoc'] = response_data['numdoc']
            item['active'] = response_data['active']
            item['fechnacpac'] = response_data['fechnacpac']
            item['idubigeores'] = response_data['idubigeores']
            item['apelpatpac'] = response_data['apelpatpac']
            item['fecha_consulta'] = fecha_consulta
            #self.logger.info(f'Se procesó el DNI {dni}.')
            #msj = 'OK'
            line += f'SUCCESS\t\n'
            yield item
        #print(f"Procesando el DNI= {dni} y apellido= '{apellido}': {msj}.")
        with open(self.filepath_log, 'ab') as file:
            file.write(line.encode("utf8"))