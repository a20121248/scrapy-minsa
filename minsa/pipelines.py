# -*- coding: utf-8 -*-

import pandas as pd
from minsa.items import PersonaItem

class csvWriterPipeline(object):
    out_path = './2_OUTPUT/'
    items_written_infogeneral = 0

    def open_spider(self, spider):
        spider_name = type(spider).__name__
        if spider_name == 'DatosPersonalesSpider':
            self.ctd_save_infogeneral = 1
            self.columnsPrincipal = ['active','fgreniec','numdoc','apelpatpac','apelmatpac','nombpac','fechnacpac','idsexo','descresubi','direccion','idubigeores','fecha_consulta']
            self.prename_infogeneral = f"{self.out_path}datos_personales_{getattr(spider, 'filename').split('.')[0]}_{getattr(spider, 'YYYYMMDD_HHMMSS')}.txt"
        self.data_encontrada_infogeneral = []

    def process_item(self, item, spider):
        if isinstance(item, PersonaItem):
            item_df = pd.DataFrame([item], columns=item.keys())

        self.items_written_infogeneral += 1
        self.data_encontrada_infogeneral.append(item_df)
        if self.items_written_infogeneral % self.ctd_save_infogeneral == 0:
            self.data_encontrada_infogeneral = self.guarda_data(self.data_encontrada_infogeneral, self.items_written_infogeneral)
        return item

    def guarda_data(self, lista_df, ctd_items=0):
        if len(lista_df)>0:
            pd.concat(lista_df).to_csv(self.prename_infogeneral, sep='\t', header=ctd_items<=self.ctd_save_infogeneral, index=False, encoding="utf-8", columns=self.columnsPrincipal, mode='a')
        return []

    def __del__(self):
        self.guarda_data(self.data_encontrada_infogeneral, self.items_written_infogeneral)
        print(25*'=','THE END',25*'=')