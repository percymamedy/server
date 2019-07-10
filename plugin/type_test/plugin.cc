/*
   Copyright (c) 2000, 2015, Oracle and/or its affiliates.
   Copyright (c) 2009, 2019, MariaDB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#include <my_global.h>
#include <sql_class.h>          // THD
#include <mysql/plugin.h>
#include "sql_type.h"


class Field_test_int8 :public Field_longlong
{
public:
  Field_test_int8(const LEX_CSTRING &name, const Record_addr &addr,
                  enum utype unireg_check_arg,
                  uint32 len_arg, bool zero_arg, bool unsigned_arg)
    :Field_longlong(addr.ptr(), len_arg, addr.null_ptr(), addr.null_bit(),
                    Field::NONE, &name, zero_arg, unsigned_arg)
  {}
  void sql_type(String &res) const
  {
    CHARSET_INFO *cs= res.charset();
    res.length(cs->cset->snprintf(cs,(char*) res.ptr(),res.alloced_length(),
               "test_int8"));
    // UNSIGNED and ZEROFILL flags are not supported by the parser yet.
    // add_zerofill_and_unsigned(res);
  }
  const Type_handler *type_handler() const;
};


class Type_handler_test_int8: public Type_handler_longlong
{
public:
  const Name name() const override
  {
    static Name name(STRING_WITH_LEN("test_int8"));
    return name;
  }
  bool Column_definition_data_type_info_image(Binary_string *to,
                                              const Column_definition &def)
                                              const override
  {
    return to->append(Type_handler_test_int8::name().lex_cstring());
  }
  Field *make_table_field(MEM_ROOT *root,
                          const LEX_CSTRING *name,
                          const Record_addr &addr,
                          const Type_all_attributes &attr,
                          TABLE *table) const override
  {
    return new (root)
           Field_test_int8(*name, addr, Field::NONE,
                           attr.max_char_length(),
                           0/*zerofill*/,
                           attr.unsigned_flag);
  }

  Field *make_table_field_from_def(TABLE_SHARE *share, MEM_ROOT *root,
                                   const LEX_CSTRING *name,
                                   const Record_addr &rec, const Bit_addr &bit,
                                   const Column_definition_attributes *attr,
                                   uint32 flags) const override
  {
    return new (root)
      Field_test_int8(*name, rec, attr->unireg_check,
                      (uint32) attr->length,
                      f_is_zerofill(attr->pack_flag) != 0,
                      f_is_dec(attr->pack_flag) == 0);
  }
};

static Type_handler_test_int8 type_handler_test_int8;


const Type_handler *Field_test_int8::type_handler() const
{
  return &type_handler_test_int8;
}


/*************************************************************************/

static struct st_mariadb_data_type data_type_test_plugin=
{
  MariaDB_DATA_TYPE_INTERFACE_VERSION,
  &type_handler_test_int8
};


maria_declare_plugin(type_geom)
{
  MariaDB_DATA_TYPE_PLUGIN,     // the plugin type (see include/mysql/plugin.h)
  &data_type_test_plugin,       // pointer to type-specific plugin descriptor
  "TEST_INT8",                  // plugin name
  "MariaDB",                    // plugin author
  "Data type TEST_INT8",        // the plugin description
  PLUGIN_LICENSE_GPL,           // the plugin license (see include/mysql/plugin.h)
  0,                            // Pointer to plugin initialization function
  0,                            // Pointer to plugin deinitialization function
  0x0100,                       // Numeric version 0xAABB means AA.BB veriosn
  NULL,                         // Status variables
  NULL,                         // System variables
  "1.0",                        // String version representation
  MariaDB_PLUGIN_MATURITY_ALPHA // Maturity (see include/mysql/plugin.h)*/
}
maria_declare_plugin_end;
