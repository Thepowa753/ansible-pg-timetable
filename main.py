#!/usr/bin/python

# Copyright: (c) 2020, Your Name <YourName@example.org>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
'''

EXAMPLES = r'''
'''

RETURN = r'''
'''

from ansible.module_utils.basic import AnsibleModule
import psycopg2


def run_module():
    module_args = dict(
        db=dict(type='str', required=True),
        job_name=dict(type='str', required=True),
        schedule=dict(type='str', required=True),
        command=dict(type='str', required=True),
        parameters=dict(type='str', required=False),
        kind=dict(type='str', required=False),
        client_name=dict(type='str', required=False),
        max_instances=dict(type='int', required=False),
        live=dict(type='bool', required=False),
        self_destruct=dict(type='bool', required=False),
        ignore_errors=dict(type='bool', required=False),
        edit_mode=dict(type='str', required=False, default="update"),  # update, drop, fail, ignore
    )

    # seed the result dict in the object
    # we primarily care about changed and state
    # changed is if this module effectively modified the target
    # state will include any data that you want your module to pass back
    # for consumption, for example, in a subsequent task
    result = dict(
        changed=False,
        original_message='',
        message='',
        my_useful_info={},
    )

    # the AnsibleModule object will be our abstraction working with Ansible
    # this includes instantiation, a couple of common attr would be the
    # args/params passed to the execution, as well as if the module
    # supports check mode
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    # if the user is working with this module in only check mode we do not
    # want to make any changes to the environment, just return the current
    # state with no modifications
    # @ToDo qui fai solo il check
    if module.check_mode:
        with psycopg2.connect() as connection:
            with connection.cursor() as cursor:
                cursor.exec("SELECT count(*) from timetable.chain where chain_name = %s;", (module.params['job_name'],))
                if cursor.fetchone()[0] != 0:
                    result['message'] = "Job already exists"
                    if module.params['edit_mode'] == "fail":
                        result['changed'] = False
                        module.fail_json(**result)
                    else:
                        result['changed'] = True
                        module.exit_json(**result)

    with psycopg2.connect() as connection:
        with connection.cursor() as cursor:
            cursor.exec("SELECT count(*) from timetable.chain where chain_name = %s;", (module.params['job_name'],))
            if cursor.fetchone()[0] != 0:
                if module.params['edit_mode'] == "fail":
                    result['message'] = "Job already exists"
                    result['changed'] = False
                    module.fail_json(**result)
                elif module.params['edit_mode'] == "ignore":
                    result['message'] = "Job already exists"
                    result['changed'] = False
                else:
                    result['changed'] = True
                    if module.params['edit_mode'] == "drop":
                        cursor.exec("select timetable.delete_job(%s);", (module.params['job_name'],))
                        query = "SELECT timetable.add_job(" \
                                "job_name            => %(job_name)s," \
                                "job_schedule        => %(schedule)s," \
                                "job_command         => %(command)s,"
                        if "parameters" in module.params:
                            query += "job_parameters      => %(parameters)s,"
                        if "kind" in module.params:
                            query += "job_kind      => %(kind)s,"
                        if "client_name" in module.params:
                            query += "job_client_name      => %(client_name)s,"
                        if "max_instances" in module.params:
                            query += "job_max_instances      => %(max_instances)s,"
                        if "live" in module.params:
                            query += "job_live      => %(live)s,"
                        if "self_destruct" in module.params:
                            query += "job_self_destruct      => %(self_destruct)s,"
                        if "ignore_errors" in module.params:
                            query += "job_ignore_errors      => %(ignore_errors)s,"
                        cursor.execute(query[:-1]+');', module.params)
                        connection.commit()
                        result['message'] = "Job dropped and reinserted"
                    if module.params['edit_mode'] == "drop":
                        # @ToDo finish the update querys
                        result['message'] = "Update still not developed"
                        result['changed'] = False
                        module.fail_json(**result)
                module.exit_json(**result)
            else:
                query = "SELECT timetable.add_job(" \
                        "job_name            => %(job_name)s," \
                        "job_schedule        => %(schedule)s," \
                        "job_command         => %(command)s,"
                if "parameters" in module.params:
                    query += "job_parameters      => %(parameters)s,"
                if "kind" in module.params:
                    query += "job_kind      => %(kind)s,"
                if "client_name" in module.params:
                    query += "job_client_name      => %(client_name)s,"
                if "max_instances" in module.params:
                    query += "job_max_instances      => %(max_instances)s,"
                if "live" in module.params:
                    query += "job_live      => %(live)s,"
                if "self_destruct" in module.params:
                    query += "job_self_destruct      => %(self_destruct)s,"
                if "ignore_errors" in module.params:
                    query += "job_ignore_errors      => %(ignore_errors)s,"
                cursor.execute(query[:-1]+');', module.params)
                connection.commit()
                result['message'] = "Job added"
                result['changed'] = True
                module.exit_json(**result)

def main():
    run_module()


if __name__ == '__main__':
    main()