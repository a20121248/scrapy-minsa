# -*- coding: utf-8 -*-

import scrapy

class PersonaItem(scrapy.Item):
    fgreniec = scrapy.Field()
    descresubi = scrapy.Field()
    apelmatpac = scrapy.Field()
    direccion = scrapy.Field()
    idsexo = scrapy.Field()
    nombpac = scrapy.Field()
    numdoc = scrapy.Field()
    active = scrapy.Field()
    fechnacpac = scrapy.Field()
    idubigeores = scrapy.Field()
    apelpatpac = scrapy.Field()
    fecha_consulta = scrapy.Field()
    pass