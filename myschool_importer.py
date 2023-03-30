import click
import requests
import json
import re
import time
import urllib.parse
import traceback
import csv
from datetime import datetime
from urllib.parse import urlparse, urljoin, parse_qs


def filter_cvs_column(value: str) -> str:
    """
    Filters out value like '=""123""'
    # https://regex101.com/r/ASMSuj/3
    """
    if value is None:
        return value

    g = re.match(r'\"=\"\"(.*)\"\"\"', value)    
    
    if g is not None:
        return g.group(1)
    else:
        return value


def string_or_null(value: str) -> str:
    
    if value is None:
        return None

    if len(value.strip()) > 0:
        return value
    else:
        return None 



@click.group()
@click.option('--debug', default=False, is_flag=True)
@click.option('--phaistos_api', default='http://localhost:8000')
@click.pass_context
def cli(ctx, debug, phaistos_api):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    ctx.obj['debug'] = debug
    ctx.obj['phaistos_api'] = phaistos_api


@cli.command()
@click.argument('report_04_01_path', type=click.File('r', encoding='cp1253'))
@click.option('--employee_am', default=None, type=str, help='AM of employee')
@click.option('--employee_afm', default=None, type=str, help='AFM of employee')
@click.option('--employee_type', default=None, type=click.Choice(['Μόνιμος', 'Αναπληρωτής', 'Αναπληρωτής ΠΔΕ']), help='employee type')
@click.option('--skip_until_am', default=None, type=int, help='skip until employee AM')
@click.option('--continue_after_am', default=None, type=int, help='continue after employee AM')
@click.pass_context
def import_employee_report_04_01(ctx, report_04_01_path, employee_am, employee_afm, employee_type, skip_until_am, continue_after_am):
    """Import myschool employee report 01 from REPORT_04_01_PATH
    
    """
    started_on = datetime.now().replace(microsecond=0)
    debug = ctx.obj.get('debug', False)
    phaistos_api = ctx.obj['phaistos_api']
    employee_resource = phaistos_api + "/api/bulk_import/myschool/employees/"
    
    employee_reader = csv.reader(report_04_01_path, delimiter=';', quotechar='|')
    row1 = next(employee_reader)  # gets the first line

    with requests.Session() as s:
        
        for row in employee_reader:

            _employee_am = row[0]
            
            if employee_am is not None and employee_am != _employee_am:
                continue
            
            _employee_afm = filter_cvs_column(row[1])
            
            if employee_afm is not None and employee_afm != _employee_afm:
                continue
            
            _employee_sex = row[2]
            _employee_last_name = row[3]
            _employee_first_name = row[4]
            _employee_father_name = row[5]
            _employee_mother_name = row[6]
            _employee_birthday = row[51]
            _employee_telephone = row[9]
            _employee_mobile = row[10]
            _employee_email = row[12]
            _employee_email_psd = row[13]
            _employee_type_name = row[47]

            if employee_type is not None and employee_type != _employee_type_name:
                continue

            _employee_current_unit_id = row[35]
            _employee_current_unit_name = row[36]
            _employee_specialization_id = row[14]
            _employee_specialization_name = row[15]
            _employee_mandatory_week_workhours = row[25]
            _employee_mk = row[19]
            _employee_bathmos = row[18]
            _employee_first_workday_date = row[32]
            _employee_fek_diorismou = row[20]
            _employee_fek_diorismou_date = row[21]
    

            # normalization
            _employee_current_unit_id = filter_cvs_column(_employee_current_unit_id)

            employee_dict = {
                'employee_am': _employee_am,
                'employee_afm': _employee_afm,
                'employee_sex': _employee_sex,
                'employee_last_name': _employee_last_name,
                'employee_first_name': _employee_first_name,
                'employee_father_name': _employee_father_name,
                'employee_mother_name': _employee_mother_name,
                'employee_telephone': _employee_telephone,
                'employee_mobile': _employee_mobile,
                'employee_email': _employee_email,
                'employee_email_psd': _employee_email_psd,
                'employee_type_name': _employee_type_name,
                'employee_current_unit_id': _employee_current_unit_id,
                'employee_current_unit_name': _employee_current_unit_name,
                'employee_specialization_id': _employee_specialization_id,
                'employee_specialization_name': _employee_specialization_name,
                'employee_mandatory_week_workhours': _employee_mandatory_week_workhours,
                'employee_mk': _employee_mk,
                'employee_bathmos': _employee_bathmos,
                'employee_first_workday_date': _employee_first_workday_date,
                'employee_fek_diorismou': _employee_fek_diorismou,
                'employee_fek_diorismou_date': _employee_fek_diorismou_date,
                'employee_birthday': _employee_birthday
            }

            employee_label = f"({employee_dict.get('employee_am')}) {employee_dict.get('employee_last_name')} {employee_dict.get('employee_first_name')} {employee_dict.get('employee_father_name')} [{employee_dict.get('employee_type_name')}]"


            if debug:
                click.echo(f"[I] request object is {json.dumps(employee_dict, ensure_ascii=False, sort_keys=True, indent=2)}")
            
            try:
                r = s.post(employee_resource, json=employee_dict)
                
                if r.status_code == 201:
                    # employee was created
                    click.echo(f"[I] successfully added employee '{employee_label}' with ID {r.json().get('id')}")
                    #click.echo(json.dumps(r.json(), sort_keys=True, indent=2))
                    
                elif r.status_code == 200:
                    # employee was updated
                    click.echo(f"[I] successfully UPDATED employee '{employee_label}' with ID {r.json().get('id')}")
                    #click.echo(json.dumps(r.json(), sort_keys=True, indent=2))
                elif r.status_code == 404:
                    # employee could not matched with phaistos
                    click.echo(f"[W] could not found employee {employee_label} in phaistos")
                    raise click.Abort()
                else:
                    click.echo(f"[W] failed inserting/updating employee '{employee_label}'")
                    click.echo(f"[W] Response : HTTP/{r.status_code}")
                    click.echo()
                    click.echo(json.dumps(r.json(), sort_keys=True, ensure_ascii=False, indent=2))
                    raise click.Abort()
                
                
                #return True

            except Exception as e:
                raise click.ClickException(e)

        
    

@cli.command()
@click.argument('report_01_07_path', type=click.File('r', encoding='cp1253'))
@click.option('--employee_am', default=None, type=str, help='AM of employee')
@click.option('--employee_afm', default=None, type=str, help='AFM of employee')
@click.option('--skip_until_am', default=None, type=int, help='skip until employee AM')
@click.option('--continue_after_am', default=None, type=int, help='continue after employee AM')
@click.pass_context
def import_employee_report_01_07(ctx, report_01_07_path, employee_am, employee_afm, skip_until_am, continue_after_am):
    """Import myschool employee report 01 from REPORT_07_01_PATH
    
    """
    started_on = datetime.now().replace(microsecond=0)
    debug = ctx.obj.get('debug', False)
    phaistos_api = ctx.obj['phaistos_api']
    employee_resource = phaistos_api + "/api/bulk_import/myschool/employees/"
    
    employee_reader = csv.reader(report_01_07_path, delimiter=';', quotechar='|')
    row1 = next(employee_reader)  # gets the first line

    with requests.Session() as s:
        
        for row in employee_reader:

            _employee_am = row[0]
            
            if employee_am is not None and employee_am != _employee_am:
                continue
            
            _employee_afm = filter_cvs_column(row[1])
            
            if employee_afm is not None and employee_afm != _employee_afm:
                continue
            
            _employee_sex = row[2]
            _employee_last_name = row[3]
            _employee_first_name = row[4]
            _employee_father_name = row[5]
            _employee_mother_name = row[6]
            _employee_birthday = row[49]
            _employee_telephone = row[9]
            _employee_mobile = row[10]
            _employee_email = row[12]
            _employee_email_psd = row[13]
            _employee_type_name = f'Διοικητικός {row[47]}'

            if _employee_type_name == 'Διοικητικός Μόνιμος':
                _employee_type_name = 'Διοικητικός'

            _employee_current_unit_id = row[35]
            _employee_current_unit_name = row[36]
            _employee_specialization_id = row[14]
            _employee_specialization_name = row[15]
            _employee_mandatory_week_workhours = row[25]
            _employee_mk = row[19]
            _employee_bathmos = row[18]
            _employee_first_workday_date = row[32]
            _employee_fek_diorismou = row[20]
            _employee_fek_diorismou_date = row[21]
    

            # normalization
            _employee_current_unit_id = filter_cvs_column(_employee_current_unit_id)

            employee_dict = {
                'employee_am': _employee_am,
                'employee_afm': _employee_afm,
                'employee_sex': _employee_sex,
                'employee_last_name': _employee_last_name,
                'employee_first_name': _employee_first_name,
                'employee_father_name': _employee_father_name,
                'employee_mother_name': _employee_mother_name,
                'employee_telephone': _employee_telephone,
                'employee_mobile': _employee_mobile,
                'employee_email': _employee_email,
                'employee_email_psd': _employee_email_psd,
                'employee_type_name': _employee_type_name,
                'employee_current_unit_id': _employee_current_unit_id,
                'employee_current_unit_name': _employee_current_unit_name,
                'employee_specialization_id': _employee_specialization_id,
                'employee_specialization_name': _employee_specialization_name,
                'employee_mandatory_week_workhours': _employee_mandatory_week_workhours,
                'employee_mk': _employee_mk,
                'employee_bathmos': _employee_bathmos,
                'employee_first_workday_date': _employee_first_workday_date,
                'employee_fek_diorismou': _employee_fek_diorismou,
                'employee_fek_diorismou_date': _employee_fek_diorismou_date,
                'employee_birthday': _employee_birthday
            }

            employee_label = f"({employee_dict.get('employee_am')}) {employee_dict.get('employee_last_name')} {employee_dict.get('employee_first_name')} {employee_dict.get('employee_father_name')} [{employee_dict.get('employee_type_name')}]"


            if debug:
                click.echo(f"[I] request object is {json.dumps(employee_dict, ensure_ascii=False, sort_keys=True, indent=2)}")
            
            try:
                r = s.post(employee_resource, json=employee_dict)
                
                if r.status_code == 201:
                    # employee was created
                    click.echo(f"[I] successfully added employee '{employee_label}' with ID {r.json().get('id')}")
                    #click.echo(json.dumps(r.json(), sort_keys=True, indent=2))
                    
                elif r.status_code == 200:
                    # employee was updated
                    click.echo(f"[I] successfully UPDATED employee '{employee_label}' with ID {r.json().get('id')}")
                    #click.echo(json.dumps(r.json(), sort_keys=True, indent=2))
                elif r.status_code == 404:
                    # employee could not matched with phaistos
                    click.echo(f"[W] could not found employee {employee_label} in phaistos")
                    raise click.Abort()
                else:
                    click.echo(f"[W] failed inserting/updating employee '{employee_label}'")
                    click.echo(f"[W] Response : HTTP/{r.status_code}")
                    click.echo()
                    click.echo(json.dumps(r.json(), sort_keys=True, ensure_ascii=False, indent=2))
                    raise click.Abort()
                
                
                #return True

            except Exception as e:
                raise click.ClickException(e)