import pandas as pd



down_dic ={'tumori maligni':['di cui tumori maligni delle labbra, cavità orale e faringe',
                            "di cui tumori maligni dell'esofago",
                            'di cui tumori maligni dello stomaco',
                            "di cui tumori maligni del colon, del retto e dell'ano",
                            'di cui tumori maligni del fegato e dei dotti biliari intraepatici',
                            'di cui tumori maligni del pancreas',
                            'di cui tumori maligni della laringe',
                            'di cui tumori maligni della trachea, dei bronchi e dei polmoni',
                            'di cui melanomi maligni della cute',
                            'di cui tumori maligni del seno',
                            'di cui tumori maligni della cervice uterina',
                            "di cui tumori maligni di altre parti dell'utero",
                            "di cui tumori maligni dell'ovaio",
                            'di cui tumori maligni della prostata',
                            'di cui tumori maligni del rene',
                            'di cui tumori maligni della vescica',
                            'di cui tumori maligni del cervello e del sistema nervoso centrale',
                            'di cui tumori maligni della tiroide',
                            'di cui morbo di hodgkin e linfomi',
                            'di cui leucemia',
                            'di cui altri tumori maligni del tessuto linfatico/ematopoietico',
                            'di cui altri tumori maligni'],
            
            'malattie ischemiche del cuore':['di cui infarto miocardico acuto',
                                            'du cui altre malattie ischemiche del cuore'],
            
            'malattie croniche delle basse vie respiratorie':['di cui asma',
                                                            'di cui altre malattie croniche delle basse vie respiratorie'],
            
            'accidenti':['di cui accidenti di trasporto',
                        'di cui cadute accidentali',
                        'di cui annegamento e sommersione accidentali',
                        'di cui avvelenamento accidentale',
                        'di cui altri accidenti']}
    

    
up_dic = {'alcune malattie infettive e parassitarie':['tubercolosi',
                                                        'aids (malattia da hiv)',
                                                        'epatite virale',
                                                        'altre malattie infettive e parassitarie'],
           'tumori': ['di cui tumori maligni delle labbra, cavità orale e faringe',
                    "di cui tumori maligni dell'esofago",
                    'di cui tumori maligni dello stomaco',
                    "di cui tumori maligni del colon, del retto e dell'ano",
                    'di cui tumori maligni del fegato e dei dotti biliari intraepatici',
                    'di cui tumori maligni del pancreas',
                    'di cui tumori maligni della laringe',
                    'di cui tumori maligni della trachea, dei bronchi e dei polmoni',
                    'di cui melanomi maligni della cute',
                    'di cui tumori maligni del seno',
                    'di cui tumori maligni della cervice uterina',
                    "di cui tumori maligni di altre parti dell'utero",
                    "di cui tumori maligni dell'ovaio",
                    'di cui tumori maligni della prostata',
                    'di cui tumori maligni del rene',
                    'di cui tumori maligni della vescica',
                    'di cui tumori maligni del cervello e del sistema nervoso centrale',
                    'di cui tumori maligni della tiroide',
                    'di cui morbo di hodgkin e linfomi',
                    'di cui leucemia',
                    'di cui altri tumori maligni del tessuto linfatico/ematopoietico',
                    'di cui altri tumori maligni',
                    'tumori maligni',
                    'tumori non maligni (benigni e di comportamento incerto)'],
           
            'malattie endocrine, nutrizionali e metaboliche':['diabete mellito',
                                                                'altre malattie endocrine, nutrizionali e metaboliche'],
            
            'disturbi psichici e comportamentali':['demenza',
                                                    'abuso di alcool (compresa psicosi alcolica)',
                                                    'dipendenza da droghe, tossicomania',
                                                    'altri disturbi psichici e comportamentali'],
            
            'malattie del sistema nervoso e degli organi di senso':['morbo di parkinson',
                                                                'malattia di alzheimer',
                                                                'altre malattie del sistema nervoso e degli organi di senso'],
            'malattie del sistema circolatorio':['malattie ischemiche del cuore',
                                                    'di cui infarto miocardico acuto',
                                                    'du cui altre malattie ischemiche del cuore',
                                                    'altre malattie del cuore',
                                                    'malattie cerebrovascolari',
                                                    'altre malattie del sistema circolatorio'],
            'malattie del sistema respiratorio':['influenza',
                                                'polmonite',
                                                'malattie croniche delle basse vie respiratorie',
                                                'di cui asma',
                                                'di cui altre malattie croniche delle basse vie respiratorie',
                                                'altre malattie del sistema respiratorio'],
            "malattie dell'apparato digerente":['ulcera dello stomaco, duodeno e digiuno',
                                                'cirrosi, fibrosi ed epatite cronica',
                                                "altre malattie dell'apparato digerente"],
            'malattie del sistema osteomuscolare e del tessuto connettivo':['artrite reumatoide a osteoartrosi',
                                                                'altre malattie del sistema osteomuscolare e del tessuto connettivo'],
            "malattie dell'apparato genitourinario":["malattie del rene e dell'uretere",
                                                    "altre malattie dell'apparato genitourinario"],
            'sintomi, segni, risultati anomali e cause mal definite':["sindrome della morte improvvisa nell'infanzia",
                                                                'cause sconosciute e non specificate',
                                                                'altri sintomi, segni, risultati anomali e cause mal definite'],
            'Covid-19':['Covid-19, virus identificato',
                        'Covid-19, virus non identificato',
                        'Covid-19, altro'],
            'cause esterne di traumatismo e avvelenamento':['accidenti',
                                                            'di cui accidenti di trasporto',
                                                            'di cui cadute accidentali',
                                                            'di cui annegamento e sommersione accidentali',
                                                            'di cui avvelenamento accidentale',
                                                            'di cui altri accidenti',
                                                            'suicidio e autolesione intenzionale',
                                                            'omicidio, aggressione',
                                                            'eventi di intento indeterminato',
                                                            'altre cause esterne di traumatismo e avvelenamento']}


class Add_ID():
    
    def __init__(self, df: pd.DataFrame):
        
        self.df = df
    
    
    def add_unique_id(self) -> list:
        unique_list = []
        for i, row in self.df.iterrows():
            name = str(row)
            unique_name = str(hash(name))
            unique_list.append(unique_name)
        return unique_list
    

class AddLevels():
          
    group_mapper={"DOWN": down_dic,
                  "UP": up_dic}
    
    
    def __init__(self, df: pd.DataFrame, roi_group:str) -> None:
        
        self.df = df
        self.roi_group = roi_group
        self.group_dic = self.group_mapper.get(roi_group)
        
        
    def invert_dict(self): 
        inverse = dict() 
        for key in self.group_dic: 
            for item in self.group_dic[key]:
                if item not in inverse: 
                    inverse[item] = key 
                else: 
                    inverse[item].append(key) 
        return inverse


    def add_groups(self):
        dic = self.invert_dict()
        group = []
        for i, row in self.df.iterrows():
            cause = row['Causa_iniziale_di_morte']
            if cause in list(dic.keys()):
                group.append(dic[cause])
            else:
                group.append(None)
        return group



            
            
            
            
            
            
    