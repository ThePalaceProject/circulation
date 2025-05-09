"""Data and functions for dealing with language names and codes."""

import re
from collections import defaultdict
from re import Pattern


class LookupTable(dict):
    """Return None on x[key] when 'key' isn't in the dictionary,
    rather than raising a ValueError.
    """

    def __getitem__(self, k):
        if k in self:
            return super().__getitem__(k)
        else:
            return None


class LanguageCodes:
    """Convert between ISO-639-2 and ISO-693-1 language codes.

    The data file comes from
    http://www.loc.gov/standards/iso639-2/ISO-639-2_utf-8.txt
    According to LOC (https://www.loc.gov/standards/iso639-2/ascii_8bits.html), the format is
    An alpha-3 (bibliographic) code, an alpha-3 (terminologic) code (when given), an alpha-2 code (when given),
    an English name, and a French name of a language are all separated by pipe (|) characters.
    """

    two_to_three = LookupTable()
    three_to_two = LookupTable()
    terminologic_to_three = LookupTable()
    english_names: dict[str, list[str]] = defaultdict(list)
    english_names_to_three = LookupTable()
    native_names: dict[str, list[str]] = defaultdict(list)

    RAW_DATA = """aar||aa|Afar|afar
abk||ab|Abkhazian|abkhaze
ace|||Achinese|aceh
ach|||Acoli|acoli
ada|||Adangme|adangme
ady|||Adyghe; Adygei|adyghé
afa|||Afro-Asiatic languages|afro-asiatiques, langues
afh|||Afrihili|afrihili
afr||af|Afrikaans|afrikaans
ain|||Ainu|aïnou
aka||ak|Akan|akan
akk|||Akkadian|akkadien
alb|sqi|sq|Albanian|albanais
ale|||Aleut|aléoute
alg|||Algonquian languages|algonquines, langues
alt|||Southern Altai|altai du Sud
amh||am|Amharic|amharique
ang|||English, Old (ca.450-1100)|anglo-saxon (ca.450-1100)
anp|||Angika|angika
apa|||Apache languages|apaches, langues
ara||ar|Arabic|arabe
arc|||Official Aramaic (700-300 BCE); Imperial Aramaic (700-300 BCE)|araméen d'empire (700-300 BCE)
arg||an|Aragonese|aragonais
arm|hye|hy|Armenian|arménien
arn|||Mapudungun; Mapuche|mapudungun; mapuche; mapuce
arp|||Arapaho|arapaho
art|||Artificial languages|artificielles, langues
arw|||Arawak|arawak
asm||as|Assamese|assamais
ast|||Asturian; Bable; Leonese; Asturleonese|asturien; bable; léonais; asturoléonais
ath|||Athapascan languages|athapascanes, langues
aus|||Australian languages|australiennes, langues
ava||av|Avaric|avar
ave||ae|Avestan|avestique
awa|||Awadhi|awadhi
aym||ay|Aymara|aymara
aze||az|Azerbaijani|azéri
bad|||Banda languages|banda, langues
bai|||Bamileke languages|bamiléké, langues
bak||ba|Bashkir|bachkir
bal|||Baluchi|baloutchi
bam||bm|Bambara|bambara
ban|||Balinese|balinais
baq|eus|eu|Basque|basque
bas|||Basa|basa
bat|||Baltic languages|baltes, langues
bej|||Beja; Bedawiyet|bedja
bel||be|Belarusian|biélorusse
bem|||Bemba|bemba
ben||bn|Bengali|bengali
ber|||Berber languages|berbères, langues
bho|||Bhojpuri|bhojpuri
bih||bh|Bihari languages|langues biharis
bik|||Bikol|bikol
bin|||Bini; Edo|bini; edo
bis||bi|Bislama|bichlamar
bla|||Siksika|blackfoot
bnt|||Bantu (Other)|bantoues, autres langues
bos||bs|Bosnian|bosniaque
bra|||Braj|braj
bre||br|Breton|breton
btk|||Batak languages|batak, langues
bua|||Buriat|bouriate
bug|||Buginese|bugi
bul||bg|Bulgarian|bulgare
bur|mya|my|Burmese|birman
byn|||Blin; Bilin|blin; bilen
cad|||Caddo|caddo
cai|||Central American Indian languages|amérindiennes de L'Amérique centrale, langues
car|||Galibi Carib|karib; galibi; carib
cat||ca|Catalan; Valencian|catalan; valencien
cau|||Caucasian languages|caucasiennes, langues
ceb|||Cebuano|cebuano
cel|||Celtic languages|celtiques, langues; celtes, langues
cha||ch|Chamorro|chamorro
chb|||Chibcha|chibcha
che||ce|Chechen|tchétchène
chg|||Chagatai|djaghataï
chi|zho|zh|Chinese|chinois
chk|||Chuukese|chuuk
chm|||Mari|mari
chn|||Chinook jargon|chinook, jargon
cho|||Choctaw|choctaw
chp|||Chipewyan; Dene Suline|chipewyan
chr|||Cherokee|cherokee
chu||cu|Church Slavic; Old Slavonic; Church Slavonic; Old Bulgarian; Old Church Slavonic|slavon d'église; vieux slave; slavon liturgique; vieux bulgare
chv||cv|Chuvash|tchouvache
chy|||Cheyenne|cheyenne
cmc|||Chamic languages|chames, langues
cop|||Coptic|copte
cor||kw|Cornish|cornique
cos||co|Corsican|corse
cpe|||Creoles and pidgins, English based|créoles et pidgins basés sur l'anglais
cpf|||Creoles and pidgins, French-based |créoles et pidgins basés sur le français
cpp|||Creoles and pidgins, Portuguese-based |créoles et pidgins basés sur le portugais
cre||cr|Cree|cree
crh|||Crimean Tatar; Crimean Turkish|tatar de Crimé
crp|||Creoles and pidgins |créoles et pidgins
csb|||Kashubian|kachoube
cus|||Cushitic languages|couchitiques, langues
cze|ces|cs|Czech|tchèque
dak|||Dakota|dakota
dan||da|Danish|danois
dar|||Dargwa|dargwa
day|||Land Dayak languages|dayak, langues
del|||Delaware|delaware
den|||Slave (Athapascan)|esclave (athapascan)
dgr|||Dogrib|dogrib
din|||Dinka|dinka
div||dv|Divehi; Dhivehi; Maldivian|maldivien
doi|||Dogri|dogri
dra|||Dravidian languages|dravidiennes, langues
dsb|||Lower Sorbian|bas-sorabe
dua|||Duala|douala
dum|||Dutch, Middle (ca.1050-1350)|néerlandais moyen (ca. 1050-1350)
dut|nld|nl|Dutch; Flemish|néerlandais; flamand
dyu|||Dyula|dioula
dzo||dz|Dzongkha|dzongkha
efi|||Efik|efik
egy|||Egyptian (Ancient)|égyptien
eka|||Ekajuk|ekajuk
elx|||Elamite|élamite
eng||en|English|anglais
enm|||English, Middle (1100-1500)|anglais moyen (1100-1500)
epo||eo|Esperanto|espéranto
est||et|Estonian|estonien
ewe||ee|Ewe|éwé
ewo|||Ewondo|éwondo
fan|||Fang|fang
fao||fo|Faroese|féroïen
fat|||Fanti|fanti
fij||fj|Fijian|fidjien
fil|||Filipino; Pilipino|filipino; pilipino
fin||fi|Finnish|finnois
fiu|||Finno-Ugrian languages|finno-ougriennes, langues
fon|||Fon|fon
fre|fra|fr|French|français
frm|||French, Middle (ca.1400-1600)|français moyen (1400-1600)
fro|||French, Old (842-ca.1400)|français ancien (842-ca.1400)
frr|||Northern Frisian|frison septentrional
frs|||Eastern Frisian|frison oriental
fry||fy|Western Frisian|frison occidental
ful||ff|Fulah|peul
fur|||Friulian|frioulan
gaa|||Ga|ga
gay|||Gayo|gayo
gba|||Gbaya|gbaya
gem|||Germanic languages|germaniques, langues
geo|kat|ka|Georgian|géorgien
ger|deu|de|German|allemand
gez|||Geez|guèze
gil|||Gilbertese|kiribati
gla||gd|Gaelic; Scottish Gaelic|gaélique; gaélique écossais
gle||ga|Irish|irlandais
glg||gl|Galician|galicien
glv||gv|Manx|manx; mannois
gmh|||German, Middle High (ca.1050-1500)|allemand, moyen haut (ca. 1050-1500)
goh|||German, Old High (ca.750-1050)|allemand, vieux haut (ca. 750-1050)
gon|||Gondi|gond
gor|||Gorontalo|gorontalo
got|||Gothic|gothique
grb|||Grebo|grebo
grc|||Greek, Ancient (to 1453)|grec ancien (jusqu'à 1453)
gre|ell|el|Greek, Modern (1453-)|grec moderne (après 1453)
grn||gn|Guarani|guarani
gsw|||Swiss German; Alemannic; Alsatian|suisse alémanique; alémanique; alsacien
guj||gu|Gujarati|goudjrati
gwi|||Gwich'in|gwich'in
hai|||Haida|haida
hat||ht|Haitian; Haitian Creole|haïtien; créole haïtien
hau||ha|Hausa|haoussa
haw|||Hawaiian|hawaïen
heb||he|Hebrew|hébreu
her||hz|Herero|herero
hil|||Hiligaynon|hiligaynon
him|||Himachali languages; Western Pahari languages|langues himachalis; langues paharis occidentales
hin||hi|Hindi|hindi
hit|||Hittite|hittite
hmn|||Hmong; Mong|hmong
hmo||ho|Hiri Motu|hiri motu
hrv||hr|Croatian|croate
hsb|||Upper Sorbian|haut-sorabe
hun||hu|Hungarian|hongrois
hup|||Hupa|hupa
iba|||Iban|iban
ibo||ig|Igbo|igbo
ice|isl|is|Icelandic|islandais
ido||io|Ido|ido
iii||ii|Sichuan Yi; Nuosu|yi de Sichuan
ijo|||Ijo languages|ijo, langues
iku||iu|Inuktitut|inuktitut
ile||ie|Interlingue; Occidental|interlingue
ilo|||Iloko|ilocano
ina||ia|Interlingua (International Auxiliary Language Association)|interlingua (langue auxiliaire internationale)
inc|||Indic languages|indo-aryennes, langues
ind||id|Indonesian|indonésien
ine|||Indo-European languages|indo-européennes, langues
inh|||Ingush|ingouche
ipk||ik|Inupiaq|inupiaq
ira|||Iranian languages|iraniennes, langues
iro|||Iroquoian languages|iroquoises, langues
ita||it|Italian|italien
jav||jv|Javanese|javanais
jbo|||Lojban|lojban
jpn||ja|Japanese|japonais
jpr|||Judeo-Persian|judéo-persan
jrb|||Judeo-Arabic|judéo-arabe
kaa|||Kara-Kalpak|karakalpak
kab|||Kabyle|kabyle
kac|||Kachin; Jingpho|kachin; jingpho
kal||kl|Kalaallisut; Greenlandic|groenlandais
kam|||Kamba|kamba
kan||kn|Kannada|kannada
kar|||Karen languages|karen, langues
kas||ks|Kashmiri|kashmiri
kau||kr|Kanuri|kanouri
kaw|||Kawi|kawi
kaz||kk|Kazakh|kazakh
kbd|||Kabardian|kabardien
kha|||Khasi|khasi
khi|||Khoisan languages|khoïsan, langues
khm||km|Central Khmer|khmer central
kho|||Khotanese; Sakan|khotanais; sakan
kik||ki|Kikuyu; Gikuyu|kikuyu
kin||rw|Kinyarwanda|rwanda
kir||ky|Kirghiz; Kyrgyz|kirghiz
kmb|||Kimbundu|kimbundu
kok|||Konkani|konkani
kom||kv|Komi|kom
kon||kg|Kongo|kongo
kor||ko|Korean|coréen
kos|||Kosraean|kosrae
kpe|||Kpelle|kpellé
krc|||Karachay-Balkar|karatchai balkar
krl|||Karelian|carélien
kro|||Kru languages|krou, langues
kru|||Kurukh|kurukh
kua||kj|Kuanyama; Kwanyama|kuanyama; kwanyama
kum|||Kumyk|koumyk
kur||ku|Kurdish|kurde
kut|||Kutenai|kutenai
lad|||Ladino|judéo-espagnol
lah|||Lahnda|lahnda
lam|||Lamba|lamba
lao||lo|Lao|lao
lat||la|Latin|latin
lav||lv|Latvian|letton
lez|||Lezghian|lezghien
lim||li|Limburgan; Limburger; Limburgish|limbourgeois
lin||ln|Lingala|lingala
lit||lt|Lithuanian|lituanien
lol|||Mongo|mongo
loz|||Lozi|lozi
ltz||lb|Luxembourgish; Letzeburgesch|luxembourgeois
lua|||Luba-Lulua|luba-lulua
lub||lu|Luba-Katanga|luba-katanga
lug||lg|Ganda|ganda
lui|||Luiseno|luiseno
lun|||Lunda|lunda
luo|||Luo (Kenya and Tanzania)|luo (Kenya et Tanzanie)
lus|||Lushai|lushai
mac|mkd|mk|Macedonian|macédonien
mad|||Madurese|madourais
mag|||Magahi|magahi
mah||mh|Marshallese|marshall
mai|||Maithili|maithili
mak|||Makasar|makassar
mal||ml|Malayalam|malayalam
man|||Mandingo|mandingue
mao|mri|mi|Maori|maori
map|||Austronesian languages|austronésiennes, langues
mar||mr|Marathi|marathe
mas|||Masai|massaï
may|msa|ms|Malay|malais
mdf|||Moksha|moksa
mdr|||Mandar|mandar
men|||Mende|mendé
mga|||Irish, Middle (900-1200)|irlandais moyen (900-1200)
mic|||Mi'kmaq; Micmac|mi'kmaq; micmac
min|||Minangkabau|minangkabau
mis|||Uncoded languages|langues non codées
mkh|||Mon-Khmer languages|môn-khmer, langues
mlg||mg|Malagasy|malgache
mlt||mt|Maltese|maltais
mnc|||Manchu|mandchou
mni|||Manipuri|manipuri
mno|||Manobo languages|manobo, langues
moh|||Mohawk|mohawk
mon||mn|Mongolian|mongol
mos|||Mossi|moré
mul|||Multiple languages|multilingue
mun|||Munda languages|mounda, langues
mus|||Creek|muskogee
mwl|||Mirandese|mirandais
mwr|||Marwari|marvari
myn|||Mayan languages|maya, langues
myv|||Erzya|erza
nah|||Nahuatl languages|nahuatl, langues
nai|||North American Indian languages|nord-amérindiennes, langues
nap|||Neapolitan|napolitain
nau||na|Nauru|nauruan
nav||nv|Navajo; Navaho|navaho
nbl||nr|Ndebele, South; South Ndebele|ndébélé du Sud
nde||nd|Ndebele, North; North Ndebele|ndébélé du Nord
ndo||ng|Ndonga|ndonga
nds|||Low German; Low Saxon; German, Low; Saxon, Low|bas allemand; bas saxon; allemand, bas; saxon, bas
nep||ne|Nepali|népalais
new|||Nepal Bhasa; Newari|nepal bhasa; newari
nia|||Nias|nias
nic|||Niger-Kordofanian languages|nigéro-kordofaniennes, langues
niu|||Niuean|niué
nno||nn|Norwegian Nynorsk; Nynorsk, Norwegian|norvégien nynorsk; nynorsk, norvégien
nob||nb|Bokmål, Norwegian; Norwegian Bokmål|norvégien bokmål
nog|||Nogai|nogaï; nogay
non|||Norse, Old|norrois, vieux
nor||no|Norwegian|norvégien
nqo|||N'Ko|n'ko
nso|||Pedi; Sepedi; Northern Sotho|pedi; sepedi; sotho du Nord
nub|||Nubian languages|nubiennes, langues
nwc|||Classical Newari; Old Newari; Classical Nepal Bhasa|newari classique
nya||ny|Chichewa; Chewa; Nyanja|chichewa; chewa; nyanja
nym|||Nyamwezi|nyamwezi
nyn|||Nyankole|nyankolé
nyo|||Nyoro|nyoro
nzi|||Nzima|nzema
oci||oc|Occitan (post 1500); Provençal|occitan (après 1500); provençal
oji||oj|Ojibwa|ojibwa
ori||or|Oriya|oriya
orm||om|Oromo|galla
osa|||Osage|osage
oss||os|Ossetian; Ossetic|ossète
ota|||Turkish, Ottoman (1500-1928)|turc ottoman (1500-1928)
oto|||Otomian languages|otomi, langues
paa|||Papuan languages|papoues, langues
pag|||Pangasinan|pangasinan
pal|||Pahlavi|pahlavi
pam|||Pampanga; Kapampangan|pampangan
pan||pa|Panjabi; Punjabi|pendjabi
pap|||Papiamento|papiamento
pau|||Palauan|palau
peo|||Persian, Old (ca.600-400 B.C.)|perse, vieux (ca. 600-400 av. J.-C.)
per|fas|fa|Persian; Farsi; Persian Farsi|persan
phi|||Philippine languages|philippines, langues
phn|||Phoenician|phénicien
pli||pi|Pali|pali
pol||pl|Polish|polonais
pon|||Pohnpeian|pohnpei
por||pt|Portuguese|portugais
pra|||Prakrit languages|prâkrit, langues
pro|||Provençal, Old (to 1500)|provençal ancien (jusqu'à 1500)
pus||ps|Pushto; Pashto|pachto
qaa-qtz|||Reserved for local use|réservée à l'usage local
que||qu|Quechua|quechua
raj|||Rajasthani|rajasthani
rap|||Rapanui|rapanui
rar|||Rarotongan; Cook Islands Maori|rarotonga; maori des îles Cook
roa|||Romance languages|romanes, langues
roh||rm|Romansh|romanche
rom|||Romany|tsigane
rum|ron|ro|Romanian; Moldavian; Moldovan|roumain; moldave
run||rn|Rundi|rundi
rup|||Aromanian; Arumanian; Macedo-Romanian|aroumain; macédo-roumain
rus||ru|Russian|russe
sad|||Sandawe|sandawe
sag||sg|Sango|sango
sah|||Yakut|iakoute
sai|||South American Indian (Other)|indiennes d'Amérique du Sud, autres langues
sal|||Salishan languages|salishennes, langues
sam|||Samaritan Aramaic|samaritain
san||sa|Sanskrit|sanskrit
sas|||Sasak|sasak
sat|||Santali|santal
scn|||Sicilian|sicilien
sco|||Scots|écossais
sel|||Selkup|selkoupe
sem|||Semitic languages|sémitiques, langues
sga|||Irish, Old (to 900)|irlandais ancien (jusqu'à 900)
sgn|||Sign Languages|langues des signes
shn|||Shan|chan
sid|||Sidamo|sidamo
sin||si|Sinhala; Sinhalese|singhalais
sio|||Siouan languages|sioux, langues
sit|||Sino-Tibetan languages|sino-tibétaines, langues
sla|||Slavic languages|slaves, langues
slo|slk|sk|Slovak|slovaque
slv||sl|Slovenian|slovène
sma|||Southern Sami|sami du Sud
sme||se|Northern Sami|sami du Nord
smi|||Sami languages|sames, langues
smj|||Lule Sami|sami de Lule
smn|||Inari Sami|sami d'Inari
smo||sm|Samoan|samoan
sms|||Skolt Sami|sami skolt
sna||sn|Shona|shona
snd||sd|Sindhi|sindhi
snk|||Soninke|soninké
sog|||Sogdian|sogdien
som||so|Somali|somali
son|||Songhai languages|songhai, langues
sot||st|Sotho, Southern|sotho du Sud
spa||es|Spanish; Castilian|espagnol; castillan
srd||sc|Sardinian|sarde
srn|||Sranan Tongo|sranan tongo
srp||sr|Serbian|serbe
srr|||Serer|sérère
ssa|||Nilo-Saharan languages|nilo-sahariennes, langues
ssw||ss|Swati|swati
suk|||Sukuma|sukuma
sun||su|Sundanese|soundanais
sus|||Susu|soussou
sux|||Sumerian|sumérien
swa||sw|Swahili|swahili
swe||sv|Swedish|suédois
syc|||Classical Syriac|syriaque classique
syr|||Syriac|syriaque
tah||ty|Tahitian|tahitien
tai|||Tai languages|tai, langues
tam||ta|Tamil|tamoul
tat||tt|Tatar|tatar
tel||te|Telugu|télougou
tem|||Timne|temne
ter|||Tereno|tereno
tet|||Tetum|tetum
tgk||tg|Tajik|tadjik
tgl||tl|Tagalog|tagalog
tha||th|Thai|thaï
tib|bod|bo|Tibetan|tibétain
tig|||Tigre|tigré
tir||ti|Tigrinya|tigrigna
tiv|||Tiv|tiv
tkl|||Tokelau|tokelau
tlh|||Klingon; tlhIngan-Hol|klingon
tli|||Tlingit|tlingit
tmh|||Tamashek|tamacheq
tog|||Tonga (Nyasa)|tonga (Nyasa)
ton||to|Tonga (Tonga Islands)|tongan (Îles Tonga)
tpi|||Tok Pisin|tok pisin
tsi|||Tsimshian|tsimshian
tsn||tn|Tswana|tswana
tso||ts|Tsonga|tsonga
tuk||tk|Turkmen|turkmène
tum|||Tumbuka|tumbuka
tup|||Tupi languages|tupi, langues
tur||tr|Turkish|turc
tut|||Altaic languages|altaïques, langues
tvl|||Tuvalu|tuvalu
twi||tw|Twi|twi
tyv|||Tuvinian|touva
udm|||Udmurt|oudmourte
uga|||Ugaritic|ougaritique
uig||ug|Uighur; Uyghur|ouïgour
ukr||uk|Ukrainian|ukrainien
umb|||Umbundu|umbundu
und|||Undetermined|indéterminée
urd||ur|Urdu|ourdou
uzb||uz|Uzbek|ouszbek
vai|||Vai|vaï
ven||ve|Venda|venda
vie||vi|Vietnamese|vietnamien
vol||vo|Volapük|volapük
vot|||Votic|vote
wak|||Wakashan languages|wakashanes, langues
wal|||Walamo|walamo
war|||Waray|waray
was|||Washo|washo
wel|cym|cy|Welsh|gallois
wen|||Sorbian languages|sorabes, langues
wln||wa|Walloon|wallon
wol||wo|Wolof|wolof
xal|||Kalmyk; Oirat|kalmouk; oïrat
xho||xh|Xhosa|xhosa
yao|||Yao|yao
yap|||Yapese|yapois
yid||yi|Yiddish|yiddish
yor||yo|Yoruba|yoruba
ypk|||Yupik languages|yupik, langues
zap|||Zapotec|zapotèque
zbl|||Blissymbols; Blissymbolics; Bliss|symboles Bliss; Bliss
zen|||Zenaga|zenaga
zgh|||Standard Moroccan Tamazight|amazighe standard marocain
zha||za|Zhuang; Chuang|zhuang; chuang
znd|||Zande languages|zandé, langues
zul||zu|Zulu|zoulou
zun|||Zuni|zuni
zxx|||No linguistic content; Not applicable|pas de contenu linguistique; non applicable
zza|||Zaza; Dimili; Dimli; Kirdki; Kirmanjki; Zazaki|zaza; dimili; dimli; kirdki; kirmanjki; zazaki"""

    NATIVE_NAMES_RAW_DATA = [
        {"code": "en", "name": "English", "nativeName": "English"},
        {"code": "fr", "name": "French", "nativeName": "français"},
        {"code": "de", "name": "German", "nativeName": "Deutsch"},
        {"code": "el", "name": "Greek, Modern", "nativeName": "Ελληνικά"},
        {"code": "hu", "name": "Hungarian", "nativeName": "Magyar"},
        {"code": "it", "name": "Italian", "nativeName": "Italiano"},
        {"code": "no", "name": "Norwegian", "nativeName": "Norsk"},
        {"code": "pl", "name": "Polish", "nativeName": "polski"},
        {"code": "pt", "name": "Portuguese", "nativeName": "Português"},
        {"code": "ru", "name": "Russian", "nativeName": "русский"},
        {
            "code": "es",
            "name": "Spanish, Castilian",
            "nativeName": "español, castellano",
        },
        {"code": "sv", "name": "Swedish", "nativeName": "svenska"},
    ]

    RESERVED_CODES = re.compile("^q[a-t][a-z]$")
    RESERVED_CODE_LABEL = "Reserved for local use"

    for raw_name_data in RAW_DATA.split("\n"):
        (
            alpha_3,
            terminologic_code,
            alpha_2,
            names_raw,
            french_names,
        ) = raw_name_data.strip().split("|")
        names = [x.strip() for x in names_raw.split(";")]
        if alpha_2:
            three_to_two[alpha_3] = alpha_2
            english_names[alpha_2] = names
            two_to_three[alpha_2] = alpha_3
        if terminologic_code:
            terminologic_to_three[terminologic_code] = alpha_3
        for name in names:
            english_names_to_three[name.lower()] = alpha_3
        english_names[alpha_3] = names

    for raw_native_name_data in NATIVE_NAMES_RAW_DATA:
        alpha_2 = raw_native_name_data["code"]
        alpha_3 = two_to_three[alpha_2]
        names = [x.strip() for x in raw_native_name_data["nativeName"].split(",")]
        native_names[alpha_2] = names
        native_names[alpha_3] = names

    @classmethod
    def iso_639_2_for_locale(cls, locale, default=None):
        """Turn a locale code into an ISO-639-2 alpha-3 language code."""
        if "-" in locale:
            language, place = locale.lower().split("-", 1)
        else:
            language = locale
        if len(language) == 3:
            if language in cls.english_names:
                # All LOC language code have an English name; it's already an alpha-3.
                return language
            elif language in cls.terminologic_to_three:
                return cls.terminologic_to_three[language]
            elif LanguageCodes.RESERVED_CODES.match(language):
                # the reserved range qaa-qtz is represented by one row in the LOC data
                return language
        if language in cls.two_to_three:
            # It's an alpha-2.
            return cls.two_to_three[language]

        return default

    @classmethod
    def bcp47_for_locale(cls, locale, default=None):
        """Turn a locale code into an ISO-639-2 code preferring alpha-2 if available, then alpha-3"""
        alpha3 = cls.iso_639_2_for_locale(locale, default=default)
        return cls.three_to_two.get(alpha3, alpha3)

    @classmethod
    def string_to_alpha_3(cls, s):
        """Try really hard to convert a string to an ISO-639-2 alpha-3 language code."""
        if not s:
            return None
        s = s.lower()
        if s in cls.english_names_to_three:
            # It's the English name of a language.
            return cls.english_names_to_three[s]

        return cls.iso_639_2_for_locale(s)

    @classmethod
    def name_for_languageset(cls, languages):
        if isinstance(languages, str):
            languages = languages.split(",")
        all_names = []
        if not languages:
            return ""
        for l in languages:
            normalized = cls.string_to_alpha_3(l)
            native_names = cls.native_names.get(normalized, [])
            if native_names:
                all_names.append(native_names[0])
            else:
                names = cls.english_names.get(normalized, [])
                if not names:
                    if normalized and LanguageCodes.RESERVED_CODES.match(normalized):
                        names.append(LanguageCodes.RESERVED_CODE_LABEL)
                    else:
                        raise ValueError("No native or English name for %s" % l)
                all_names.append(names[0])
        if len(all_names) == 1:
            return all_names[0]
        return "/".join(all_names)


class LanguageNames:
    """Utilities for converting between human-readable language names and codes.

    LanguageNames.name_re is a regular expression that matches the
    English or native-language name of nearly any language known to
    LanguageCodes.

    LanguageNames.name_to_codes is a dictionary mapping lowercase
    human-readable names to ISO-639-2 language codes.
    """

    irrelevant_suffixes = [" languages"]

    ignore = {"No linguistic content", "Not applicable", "Uncoded"}

    number = re.compile("[0-9]")
    parentheses = re.compile(r"\([^)]+\)")

    name_to_codes: dict[str, list[str]]
    name_re: Pattern

    @classmethod
    def _process(cls, human_readable_name, alpha):
        if not alpha or human_readable_name in cls.ignore:
            # Some names should be ignored altogether.
            return None, None

        if cls.number.search(human_readable_name):
            # This language is associated with a historical period.
            # For now, just ignore it -- books generally aren't
            # classified under these languages and people generally
            # won't type in those specific dates.
            return None, None

        if len(alpha) == 2:
            alpha = LanguageCodes.two_to_three[alpha]

        # Remove parentheses, e.g. turning "Bantu (Other)" into "Bantu"
        human_readable_name = cls.parentheses.sub("", human_readable_name)

        for suffix in cls.irrelevant_suffixes:
            # Some suffixes are not relevant for our purposes.
            # For instance, "Himachali languages" is best handled
            # as "Himachali".
            if human_readable_name.endswith(suffix):
                human_readable_name = human_readable_name[: -len(suffix)]
        return human_readable_name.strip().lower(), alpha

    @classmethod
    def _build_name_to_codes(cls):
        name_to_codes = defaultdict(set)

        def add(name, alpha):
            """Helper to add a language to name_to_codes."""
            name, alpha = cls._process(name, alpha)
            if name:
                name_to_codes[name].add(alpha)

        # Process the English-language names found in the ISO spec.
        for alpha, name_list in list(LanguageCodes.english_names.items()):
            for names in name_list:
                for name in names.split(";"):
                    add(name, alpha)

        # Add a couple of languages that were incorrectly excluded by the
        # "no dates" rule.
        for name, alpha in (("greek", "el"), ("occitan", "oc")):
            add(name, alpha)

        # Process the native-language names found in NATIVE_NAMES_RAW_DATA.
        for item in LanguageCodes.NATIVE_NAMES_RAW_DATA:
            add(item["nativeName"], item["code"])

        # Add native-language names without diacritics, for people who
        # are typing on an English-language keyboard.
        for name, alpha in (
            ("francais", "fr"),
            ("espanol", "es"),
            ("portugues", "pt"),
            ("castellano", "es"),
        ):
            add(name, alpha)
        return name_to_codes

    @classmethod
    def _build_name_re(cls):
        return re.compile(
            r"(\b%s\b)" % r"\b|\b".join(list(cls.name_to_codes.keys())), re.I
        )


# Instantiate the class variables.
LanguageNames.name_to_codes = LanguageNames._build_name_to_codes()
LanguageNames.name_re = LanguageNames._build_name_re()
