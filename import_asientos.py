# -*- coding: utf-8 -*-
import sys
import odoorpc
import unicodecsv
import openpyxl
import pickle
import time
from subprocess import Popen, PIPE
from datetime import datetime
import pandas
from math import isclose
import configparser

#reload(sys)
#sys.setdefaultencoding('utf8')

config = configparser.ConfigParser()
config.read('config_import.cfg')

SERVER = config['connection']['server']
DATABASE = config['connection']['database']
USERNAME = config['connection']['user']
PASSWORD = config['connection']['passwd']
PROTOCOL = config['connection']['protocol']
PORT = int(config['connection']['port'])

COMPANY_ID = int(config['data']['company_id'])
JOURNAL_ID = int(config['data']['default_journal_id'])
SALE_JOURNAL_ID = int(config['data']['sale_journal_id'])
PURCHASE_JOURNAL_ID = int(config['data']['purchase_journal_id'])
IVA_ACCOUNT_SALE = int(config['data']['iva_account_sale'])
IVA_ACCOUNT_PURCHASE = int(config['data']['iva_account_purchase'])

JOURNAL_FILE = config['files']['journal_file']
VAT_FILE = config['files']['vat_file']


CSV_IGNNORE_HEAD = False
HEAD_NUMBER_LINES = 2
DEBUG = True  # Solo importa las 100 primeras lineas
NCORES = 4

odoo = odoorpc.ODOO(SERVER, PROTOCOL, PORT, timeout=12000)
odoo.login(DATABASE, USERNAME, PASSWORD)

account_move_obj = odoo.env['account.move']
line_obj = odoo.env['account.move.line']
journal_obj = odoo.env['account.journal']
tax_obj = odoo.env['account.tax']
#iva_sale_account_id = tax_obj.search([('account_id', '=', IVA_ACCOUNT_SALE)])[0]
#iva_purchase_account_id = tax_obj.search([('account_id', '=', IVA_ACCOUNT_PURCHASE)])[0]

tipos_venta = {}
tipos_venta['21.0'] = tax_obj.search([('name', '=', 'IVA 21% (Bienes)')])[0]
tipos_venta['10.0'] = tax_obj.search([('name', '=', 'IVA 10% (Bienes)')])[0]
tipos_venta['4.0'] = tax_obj.search([('name', '=', 'IVA 4% (Bienes)')])[0]
tipos_compra = {}
tipos_compra['21.0'] = tax_obj.search([('name', '=', '21% IVA soportado (bienes corrientes)')])[0]
tipos_compra['10.0'] = tax_obj.search([('name', '=', '10% IVA soportado (bienes corrientes)')])[0]
tipos_compra['4.0'] = tax_obj.search([('name', '=', '4% IVA soportado (bienes corrientes)')])[0]
tipos_intra = {}
tipos_intra['21.0'] = tax_obj.search([('name', '=', 'IVA 21% Adquisición Intracomunitaria. Bienes corrientes')])[0]
tipos_intra['21.0_1'] = tax_obj.search([('name', '=', 'IVA 21% Intracomunitario. Bienes corrientes (1)')])[0]
tipos_intra['21.0_2'] = tax_obj.search([('name', '=', 'IVA 21% Intracomunitario. Bienes corrientes (2)')])[0]
tipos_intra['10.0'] = tax_obj.search([('name', '=', 'IVA 10% Adquisición Intracomunitario. Bienes corrientes')])[0]
tipos_intra['10.0_1'] = tax_obj.search([('name', '=', 'IVA 10% Intracomunitario. Bienes corrientes (1)')])[0]
tipos_intra['10.0_2'] = tax_obj.search([('name', '=', 'IVA 10% Intracomunitario. Bienes corrientes (2)')])[0]
tipos_intra['4.0'] = tax_obj.search([('name', '=', 'IVA 4% Adquisición Intracomunitario. Bienes corrientes')])[0]
tipos_intra['4.0_1'] = tax_obj.search([('name', '=', 'IVA 4% Intracomunitario. Bienes corrientes (1)')])[0]
tipos_intra['4.0_2'] = tax_obj.search([('name', '=', 'IVA 4% Intracomunitario. Bienes corrientes (2)')])[0]
tipos_intra_venta = tax_obj.search([('name', '=', 'IVA 0% Entregas Intracomunitarias exentas')])[0]

date = '2020-01-01'

emitidas = pandas.read_excel(VAT_FILE, 'EXPEDIDAS')
recibidas = pandas.read_excel(VAT_FILE, 'RECIBIDAS')

def get(row, column, ttype='str'):
    def format_str(value):
        return str(value)

    def format_float(value):
        try:
            return float(value)
        except:
            try:
                return float(str(value).replace(',', '.'))
            except:
                try:
                    import string
                    all = string.maketrans('', '')
                    nodigs = all.translate(all, string.digits)
                    return float(str(value).translate(all, nodigs))
                except:
                    return 0.00

    cols = list('abcdefghijklmnopqrstuvwxyz')
    icol = 0
    ### P.ej: 'CCD' -> icol = 3x26^2 + 3x26^1 + 4x26^0
    exp = len(column)-1
    for col in column.lower():
        icol += (cols.index(col)+1)*(len(cols)**exp)
        exp -= 1
    icol -= 1

    try:
        value = row[icol]
        if ttype == 'str':
            return value
        elif ttype == 'float':
            return format_float(value)
    except:
        return None


def confirm(question, default='yes'):
    valid = {'y': True, 'ye': True, 'yes': True, 's': True,
             'si': True, 'sí': True, 'n': False, 'no': False}

    if default not in valid:
        prompt = " [y/n] "
    elif valid[default]:
        prompt = " [Y/n] "
    else:
        prompt = " [y/N] "

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write(
                "Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def get_dif_account_id():
    account_obj = odoo.env['account.account']
    # Código 1000000 capital social
    account_ids = account_obj.search([('code', '=', '100000000')])
    account_id = account_ids and account_ids[0]
    return account_id


def get_partner(old_account_code, account_code, account_name):
    partner_id = False
    partner_ids = odoo.env['res.partner'].search([('ref', '=', old_account_code)])
    if partner_ids:
        if len(partner_ids) > 1:
            print('referencia duplicada!----------------------')
            print(account_name)
            # import pdb; pdb.set_trace()
        partner_id = partner_ids and partner_ids[0]
        return partner_id
    customer = False
    supplier = False
    if account_code[:3] == '430':
        customer = True
    elif account_code[:3] in ['400','410']:
        supplier = True
    partner_ids = odoo.env['res.partner'].search([('name', '=', account_name),('parent_id', '=', False)])
    if partner_ids:
        if len(partner_ids) > 1:
            print('nombre duplicado!----------------------')
            print(account_name)
            #import pdb; pdb.set_trace()
        partner_id = partner_ids and partner_ids[0]
        odoo.env['res.partner'].write([partner_id], {'ref': old_account_code})
        return partner_id
    
    partner_id = odoo.env['res.partner'].create({
        'name': account_name,
        'company_type': 'company',
        'ref': old_account_code,
        'customer': customer,
        'supplier': supplier
    })
    return partner_id


def get_partner_and_account_id(old_account_code, account_code, account_name):
    account_obj = odoo.env['account.account']
    partner_id = False
    account_id = False
    parent_code = '%s00' % (account_code[:4])
    if account_code[:3] in ['400', '410', '430']:
        # Cliente o proveedor o acreedor
        partner_id = get_partner(old_account_code, account_code, account_name)
        parent_code = '%s000000' % (account_code[:3])
        account_code = parent_code
    account_ids = account_obj.search([('code', '=', account_code)])
    account_id = account_ids and account_ids[0]
    if not account_id:
        parent_account_ids = account_obj.search([('code', '=', parent_code)])
        parent_account_id = parent_account_ids and parent_account_ids[0]
        if not parent_account_id:
            parent_code = '%s000000' % (account_code[:3])
            parent_account_ids = account_obj.search([('code', '=', parent_code)])
            parent_account_id = parent_account_ids and parent_account_ids[0]
            if not parent_account_id:
                parent_code = '%s0000000' % (account_code[:2])
                parent_account_ids = account_obj.search([('code', '=', parent_code)])
                parent_account_id = parent_account_ids and parent_account_ids[0]
        try:
            account_id = account_obj.copy(parent_account_id, {
                'code': account_code,
                'name': account_name
            })
        except Exception as e:

            print('> Exception {}'.format(e))
            print('ERROR copiando cuenta')
            print(account_code)
            
    if not account_id:
        print('cuenta no encontrada')
        print(account_code)
        
    return partner_id, account_id, account_code


def get_bank_journal_id(bank_account_code):
    journal_obj = odoo.env['account.journal']
    account_id = odoo.env['account.account'].search([('code', '=', bank_account_code)])[0]
    bank_journal_ids = journal_obj.search([
        ('type', '=', 'bank'),
        ('default_debit_account_id', '=', account_id),
        ('default_credit_account_id', '=', account_id)
    ])
    if bank_journal_ids:
        return bank_journal_ids[0]
    else:
        # Si no lo encuentra crea el diario de tipo de banco 
        bank_journal_id = journal_obj.search([
            ('type', '=', 'bank'),
            ('default_debit_account_id', '=', bank_account_code[:3]+'000001'),
            ('default_credit_account_id', '=', bank_account_code[:3]+'000001')
        ], limit=1)
        journal_id = journal_obj.copy(bank_journal_id[0], {
            'default_debit_account_id': account_id,
            'default_credit_account_id': account_id,
        })
        journal = journal_obj.browse(journal_id)
        journal.write({
            'name': 'Banco%s' % (bank_account_code[-2:]),
            'code': 'BNK%s' % (bank_account_code[-2:])
        })
        return journal_id


def crea_apunte(row):
    old_account_code =  row[8].value
    concept = row[6].value
    account_code = old_account_code
   
    if account_code and account_code != '':
        account_name = row[7].value
        print(account_name)
        account_code = account_code[:6]+account_code[-3:]
        print(account_code)
        debit = row[9].value
        credit = row[10].value
        date = datetime.strptime(row[0].value, '%d/%m/%Y')

        partner_id, account_id, account_code = get_partner_and_account_id(old_account_code, account_code, account_name)


        dif_float = debit - credit
        dif = round(dif_float, 2)

        if dif > 0:
            debit = dif
            credit = 0.0
        elif dif < 0:
            debit = 0.0
            credit = 0 - dif
        else:
            debit = 0.0
            credit = 0.0

        # if account_code[:3] == '430' and (credit > 0):
        #     actual_journal = sales_journal_id
        # if account_code[:3] in ['400', '410'] and (debit > 0):
        #     actual_journal = purchases_journal_id
        # if account_code[:3] == '572':  # and actual_journal not in [sales_journal_id, purchases_journal_id]:
        #     actual_journal = get_bank_journal_id(account_code, account_id) or JOURNAL_ID
        if row[36].value:
            if row[35].value:
                nfactura = str(row[35].value) + str(row[36].value)
            else:
                nfactura = str(row[36].value)
        else:
            nfactura = ''
        line = (account_code, nfactura, (0, 0, {
                'partner_id': partner_id,
                'account_id': account_id,
                'date': date.strftime("%Y-%m-%d"),
                'name': concept,
                'debit': debit,
                'credit': credit,
                #'journal_id': actual_journal
            }))
        return line
    
    
def get_taxes(lines):
    iva_venta_check = True if True in list(map(lambda x: True if x[0][:3] in['477']  else False  , lines)) else False
    iva_compra_check = True if True in list(map(lambda x: True if x[0][:3] in['472']  else False  , lines)) else False
    base = 0
    cuota = 0
    linea_cuota = []
    if iva_venta_check and iva_compra_check:
        # Intracomunitarias
        print("COMPRA INTRACOMUNITARIA")
        
        linea_proveedor = list(filter(lambda x: x[0][:2] == '40' or  x[0][:2] == '41' , lines))
        if linea_proveedor:
            nfactura = linea_proveedor[0][1]
            total = linea_proveedor[0][2][2]['debit'] or linea_proveedor[0][2][2]['credit']
        else:
            nfactura = False
        if nfactura:
            print("Nfactura PROVEEDOR INTRA:  %s" %  nfactura)
            new_lines = []
            for line in lines:
                if line[0][:1] == '6':
                    lfs = recibidas[(recibidas['Nfactura'] == int(nfactura)) & 
                                    (recibidas['Fecha Expedicion'] == line[2][2]['date']) & 
                                    (recibidas['Total Factura'] == total)]
                    if len(lfs) == 1:
                        print("INTRACOMUNITARIA: Una sola cuota")
                        tipo = str(lfs.iloc[0]['Tipo de IVA'])
                        
                        line[2][2]['tax_ids'] = [(6, 0, [tipos_intra[tipo], tipos_intra[tipo+"_1"], tipos_intra[tipo+"_2"]])]
                        linea_cuota_1 = list(filter(lambda x: x[0][:3] in ['472'] , lines))[0]
                        linea_cuota_2 = list(filter(lambda x: x[0][:3] in ['477'] , lines))[0]
                        linea_cuota_1[2][2]['tax_line_id'] = tipos_intra[tipo+"_1"]
                        linea_cuota_2[2][2]['tax_line_id'] = tipos_intra[tipo+"_2"]
                    elif len(lfs) > 1 :
                        first = True
                        print("INTRACOMUNITARIA: Varias cuotas")
                        for index, lf in lfs.iterrows():
                            tipo = str(lf['Tipo de IVA'])
                            
                            if lf['Base Imponible'] < 0:
                                debit =  0
                                credit = -1 * lf['Base Imponible']
                            else:
                                debit =  lf['Base Imponible']
                                credit = 0
                        
                            if first:
                                print("INTRACOMUNITARIA: Primera Linea BASE %s" % tipo)
                                first_line = line
                                line[2][2]['tax_ids'] = [(6, 0 , [tipos_intra[tipo], tipos_intra[tipo+"_1"], tipos_intra[tipo+"_2"]])]
                                line[2][2]['debit'] = debit
                                line[2][2]['credit'] = credit
                            
                                first = False
                            else:
                                print("INTRACOMUNITARIA: Nueva Linea BASE %s" % tipo)
                                first_line = line
                                new_line = (line[0], nfactura, (0, 0, {
                                                'partner_id': first_line[2][2]['partner_id'],
                                                'account_id': first_line[2][2]['account_id'],
                                                'date': first_line[2][2]['date'],
                                                'name': first_line[2][2]['name'],
                                                'debit': debit,
                                                'credit': credit,
                                                'tax_ids':  [(6, 0 ,[tipos_intra[tipo], tipos_intra[tipo+"_1"], tipos_intra[tipo+"_2"]])]
                                            }))
                                new_lines.append(new_line)
                    
                            # Busca lineas de cuota
                            if lf['Cuota IVA Soportado'] < 0:
                                debit =  0
                                credit = -1 * lf['Cuota IVA Soportado']
                            else:
                                debit =  lf['Cuota IVA Soportado']
                                credit = 0
                            
                            linea_cuota_1 = list(filter(lambda x: x[0][:3] in ['472'] and (isclose(x[2][2]['debit'], debit) and isclose(x[2][2]['credit'], credit)), lines))[0]
                            linea_cuota_2 = list(filter(lambda x: x[0][:3] in ['477'] and (isclose(x[2][2]['debit'], credit) and isclose(x[2][2]['credit'], debit)), lines))[0]
                            linea_cuota_1[2][2]['tax_line_id'] = tipos_intra[tipo+"_1"]
                            linea_cuota_1[2][2]['debit'] = debit
                            linea_cuota_1[2][2]['credit'] = credit
                            linea_cuota_2[2][2]['tax_line_id'] = tipos_intra[tipo+"_2"]
                            linea_cuota_2[2][2]['debit'] = debit
                            linea_cuota_2[2][2]['credit'] = credit
                    else:
                        print("Factura INTRACOMUNITARIA %s no encontrada" % nfactura)
            for new_line in new_lines:
                lines.append(new_line)    
            return
        return
    
    if iva_venta_check:
        linea_cliente = list(filter(lambda x: x[0][:2] in ['43'] , lines))
        if linea_cliente:
            nfactura = linea_cliente[0][1]
        else:
            nfactura = False
        if nfactura:
            print("Nfactura:  %s" % nfactura)
            new_lines = []
            for line in lines:
                if line[0][:1] == '7':
                    lfs = emitidas[(emitidas['Nfactura'] == nfactura) & (emitidas['Fecha Expedicion'] == line[2][2]['date'])]
                    if len(lfs) == 1:
                        print("Una sola cuota" )
                        tipo = str(lfs.iloc[0]['Tipo de IVA'])
                        
                        line[2][2]['tax_ids'] = [(6, 0, [tipos_venta[tipo]])]
                        linea_cuota = list(filter(lambda x: x[0][:2] in ['47'] , lines))[0]
                        linea_cuota[2][2]['tax_line_id'] = tipos_venta[tipo]
                    elif len(lfs) > 1 :
                        first = False
                        print("Varias cuotas")
                        for index, lf in lfs.iterrows():
                            tipo = str(lf['Tipo de IVA'])
                            
                            if lf['Base Imponible'] < 0:
                                debit =  -1 * lf['Base Imponible']
                                credit = 0
                            else:
                                debit =  0
                                credit = lf['Base Imponible']
                        
                            if not first:
                                print("Primera Linea BASE %s "% tipo)
                                first_line = line
                                line[2][2]['tax_ids'] = [(6, 0 , [tipos_venta[tipo]])]
                                line[2][2]['debit'] = debit
                                line[2][2]['credit'] = credit
                            
                                first = True
                            else:
                                print("Nueva Linea BASE %s" % tipo)
                                first_line = line
                                new_line = (line[0], nfactura, (0, 0, {
                                                'partner_id': first_line[2][2]['partner_id'],
                                                'account_id': first_line[2][2]['account_id'],
                                                'date': first_line[2][2]['date'],
                                                'name': first_line[2][2]['name'],
                                                'debit': debit,
                                                'credit': credit,
                                                'tax_ids':  [(6, 0 , [tipos_venta[tipo]])]
                                            }))
                                new_lines.append(new_line)
                    
                            # Busca lineas de cuota
                            if lf['Cuota IVA Repercutida'] < 0:
                                debit =  -1 * lf['Cuota IVA Repercutida']
                                credit = 0
                            else:
                                debit =  0
                                credit = lf['Cuota IVA Repercutida']
                            
                            linea_cuota = list(filter(lambda x: x[0][:2] in ['47'] and (x[2][2]['debit'] == debit and x[2][2]['credit'] == credit), lines))[0]
                            linea_cuota[2][2]['tax_line_id'] = tipos_venta[tipo]
                            linea_cuota[2][2]['debit'] = debit
                            linea_cuota[2][2]['credit'] = credit
            for new_line in new_lines:
                lines.append(new_line)    
            return
    if iva_compra_check:
        linea_proveedor = list(filter(lambda x: x[0][:2] == '40' or  x[0][:2] == '41' , lines))
        if linea_proveedor:
            nfactura = linea_proveedor[0][1]
            total = linea_proveedor[0][2][2]['debit'] or linea_proveedor[0][2][2]['credit']
        else:
            nfactura = False
        if nfactura:
            print("Nfactura PROVEEDOR:  %s" % nfactura)
            new_lines = []
            for line in lines:
                if line[0][:1] == '6':
                    lfs = recibidas[(recibidas['Nfactura'] == int(nfactura)) & 
                                    (recibidas['Fecha Expedicion'] == line[2][2]['date']) & 
                                    (recibidas['Total Factura'] == total)]
                    if len(lfs) == 1:
                        print("COMPRA: Una sola cuota")
                        tipo = str(lfs.iloc[0]['Tipo de IVA'])
                        
                        line[2][2]['tax_ids'] = [(6, 0, [tipos_compra[tipo]])]
                        linea_cuota = list(filter(lambda x: x[0][:2] in ['47'] , lines))[0]
                        linea_cuota[2][2]['tax_line_id'] = tipos_compra[tipo]
                    elif len(lfs) > 1 :
                        first = False
                        print("COMPRA: Varias cuotas")
                        for index, lf in lfs.iterrows():
                            tipo = str(lf['Tipo de IVA'])
                            
                            if lf['Base Imponible'] < 0:
                                debit =  0
                                credit = -1 * lf['Base Imponible']
                            else:
                                debit =  lf['Base Imponible']
                                credit = 0
                            
                            if not first:
                                
                                first_line = line
                                if tipo:
                                    print("COMPRA: Primera Linea BASE %s" % tipo)
                                    line[2][2]['tax_ids'] = [(6, 0 , [tipos_compra[tipo]])]
                                else:
                                    print("COMPRA: Primera Linea BASE sin tipo" )
                                line[2][2]['debit'] = debit
                                line[2][2]['credit'] = credit
                            
                                first = True
                            else:
                                
                                first_line = line
                                if tipo and tipo != 'nan':
                                    print("COMPRA: Nueva Linea BASE %s" % tipo)
                                    taxes = [(6, 0 , [tipos_compra[tipo]])]
                                else:
                                    print("COMPRA: Nueva Linea BASE sin tipo" )
                                    taxes = []
                                new_line = (line[0], nfactura, (0, 0, {
                                                'partner_id': first_line[2][2]['partner_id'],
                                                'account_id': first_line[2][2]['account_id'],
                                                'date': first_line[2][2]['date'],
                                                'name': first_line[2][2]['name'],
                                                'debit': debit,
                                                'credit': credit,
                                                'tax_ids': taxes
                                            }))
                                new_lines.append(new_line)
                    
                            # Busca lineas de cuota
                            if tipo and tipo != 'nan':
                            
                                if lf['Cuota IVA Soportado'] < 0:
                                    debit =  0
                                    credit = -1 * lf['Cuota IVA Soportado']
                                else:
                                    debit =  lf['Cuota IVA Soportado']
                                    credit = 0
                            
                                linea_cuota = list(filter(lambda x: x[0][:2] in ['47'] and (x[2][2]['debit'] == debit and x[2][2]['credit'] == credit), lines))[0]
                                linea_cuota[2][2]['tax_line_id'] = tipos_compra[tipo]
                                linea_cuota[2][2]['debit'] = debit
                                linea_cuota[2][2]['credit'] = credit
                    else:
                        print("Factura compra %s no encontrada" % nfactura)
            for new_line in new_lines:
                lines.append(new_line)    
            return
        
        
    iva_venta_intra_check = True if True in list(map(lambda x: True if x[0] in ['700002']  else False  , lines)) else False
    if iva_venta_intra_check:
        linea_base = list(filter(lambda x: x[0] == '700002' , lines))
        if linea_base:
            print("Añade impuesto a base intracomunitaria")
            linea_base[0][2][2]['tax_ids'] = [(6, 0 , [tipos_intra_venta])]
            
       
def crea_asiento(lines, number):

    journal_id = get_journal(lines)
    get_taxes(lines)
    lines = list(map(lambda x: x[2] , lines))

    account_move_id = account_move_obj.create({
        'name': number,
        'journal_id': journal_id,
        'date': lines[0][2]['date'],
        'company_id': COMPANY_ID,
        'line_ids': lines
    })
    account_move_obj.env.commit()
    
def get_journal(lines):
    accounts = list(map(lambda x: x[0] , lines))
    sale_check =  True if True in list(map(lambda x: True if x[:3] == '430' else False  , accounts)) else False
    purchase_check = True if True in list(map(lambda x: True if x[:3] in ['400', '410'] else False  , accounts)) else False
    bank_check = True if True in list(map(lambda x: True if x[:3] == '572' else False  , accounts)) else False
    
    if sale_check and purchase_check and bank_check:
        return JOURNAL_ID
    if bank_check:
        for account in accounts:
            if account[:3] == '572':
                bank_account_code = account
        return get_bank_journal_id(bank_account_code) or JOURNAL_ID
    if sale_check and not purchase_check:
        return SALE_JOURNAL_ID
    if not sale_check and purchase_check:
        return PURCHASE_JOURNAL_ID
    return JOURNAL_ID
    
 

if confirm("Importar asientos desde '%s' ?" % JOURNAL_FILE):
    
    excel_document = openpyxl.load_workbook(JOURNAL_FILE)
    sheet = excel_document.get_sheet_by_name('Hoja1')
    
    all_rows = sheet.rows
    total = sheet.max_row
    n = 0
    prev_number = None
    lines = []
    for row in all_rows:
        
        if row[1].value != None and row[1].value.isdigit():
            print("IMPORTANDO %d de %d " % (n , total))
            line = crea_apunte(row)
            if row[1].value != prev_number and lines:
                print("ESCRIBE ASIENTO")
                crea_asiento(lines, prev_number)
                print(n)
                lines=[]
                    
            lines.append(line)
            prev_number = row[1].value
        n = n + 1
                
