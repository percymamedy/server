#include "mariadb.h"
#include "sql_base.h"
#include "sql_select.h"

/**
  @file
   Check if SELECT list and HAVING fields are used in GROUP BY
   or are functionally dependent on fields used in GROUP BY.

   Let's call fields that are used in GROUP BY 'gb' fields and
   fields that are functionally dependent on 'gb' fields 'fd'
   fields. Fields that are either 'gb' or 'fd' or functionally
   dependent on 'fd' will be called 'allowed' fields. 'Allowed'
   fields are allowed to be used in SELECT list and HAVING.

   Field F2 is called functionally dependent on some other field F1
   if such a rule holds: if two values of F1 are equal (or both NULL)
   then two corresponding values of F2 are also equal or both NULL.
   F1 and F2 can also be groups of fields:
   (F11, ..., F1n) and (F21, ..., F2m).

   Functionally dependent fields can be extracted from the WHERE clause
   equalities. Current implementation is limited to the following equalities:

   F2 = g(H11, ... H1n), where

   (H11, ..., H1n) are some functions of 'allowed' fields and/or 'allowed'
                   fields and/or constants.
   g               is some function. It can be identity function.
   F2              is some non-'allowed' field.

   Work if 'only_full_group_by' mode is set only.
*/


/**
  This class is used to store fields used in the equality and this
  equality itself. This information is used in extraction of new
  functionally dependent fields.
*/

class Item_equal_fd_info :public Sql_alloc
{
public:
  Item_func_eq *equal;  /* Equality itself */
  List<Field> fields_l;  /* Fields used in the left part of the equality */
  List<Field> fields_r;  /* Fields used in the right part of the equality */
  Item_equal_fd_info(Item_func_eq *eq, List<Field> flds_l, List<Field> flds_r)
    : equal(eq), fields_l(flds_l), fields_r(flds_r) {}
};


/**
  Check if all 'key' parts are 'allowed' fields.
  If so return true.
*/

static bool are_key_fields_allowed(KEY *key)
{
Item *item_arg= 0;
  for (uint i= 0; i < key->user_defined_key_parts; ++i)
  {
    if (!key->key_part[i].field->
         excl_func_dep_on_grouping_fields(0, 0, &item_arg))
      return false;
  }
  return true;
}


/**
  @brief
    Check if PRIMARY or UNIQUE keys fields are 'allowed'

  @param
    sl  current select

  @details
    For each table used in the FROM list of SELECT sl check
    its PRIMARY and UNIQUE keys.
    If some table key consists of 'allowed' fields only then
    all fields of this table are 'allowed'.

  @retval
    true   if new 'allowed' fields are extracted
    false  otherwise
*/

static
bool check_allowed_unique_keys(st_select_lex *sl)
{
  List_iterator<TABLE_LIST> it(sl->leaf_tables);
  TABLE_LIST *tbl;
  bool fields_extracted= false;
  while ((tbl= it++))
  {
    if (!tbl->table)
      continue;
    /* Check if all fields of this table are already said to be 'allowed'. */
    if (bitmap_is_set_all(&tbl->table->tmp_set))
      continue;
    /* Check if PRIMARY key fields are 'allowed'. */
    if (tbl->table->s->primary_key < MAX_KEY)
    {
      KEY *pk= &tbl->table->key_info[tbl->table->s->primary_key];
      if (are_key_fields_allowed(pk))
      {
        bitmap_set_all(&tbl->table->tmp_set);
        fields_extracted= true;
        continue;
      }
    }
    /* Check if UNIQUE keys fields are 'allowed' */
    KEY *end= tbl->table->key_info + tbl->table->s->keys;
    for (KEY *k= tbl->table->key_info; k < end; k++)
      if ((k->flags & HA_NOSAME) && are_key_fields_allowed(k))
      {
        bitmap_set_all(&tbl->table->tmp_set);
        fields_extracted= true;
        break;
      }
  }
  return fields_extracted;
}


/**
  @brief
    Check if materialized derived tables and views fields are 'allowed'

  @param
    mat_derived  list of materialized derived tables and views

  @details
    Check if some field of materialized derived table or view (MDV)
    is 'allowed' in the SELECT sl (SELECT where this MDV is used).
    If so all fields of this MDV can be used in sl.

  @note
    check_func_dep() method is called for SELECTs that define
    MDVs before it is called for SELECTs where these MDVs are used.
    So when MDVs are used the fact that all their fields are 'allowed'
    can be used. The above can be translated into the following:
    if some MDV field is found to be 'allowed' in the SELECT sl
    (SELECT where this MDV is used) then all fields of this MDV
    are 'allowed' in sl.

  @retval
    true   if new 'allowed' fields are extracted
    false  otherwise
*/

static
bool check_allowed_materialized_derived(List<TABLE_LIST> *mat_derived)
{
  if (mat_derived->is_empty())
    return false;
  List_iterator<TABLE_LIST> it(*mat_derived);
  TABLE_LIST *tbl;
  uint tabs_cnt= mat_derived->elements;
  while ((tbl= it++))
  {
    /*
      Check if there is some derived table or view field that is found
      to be 'allowed'.
    */
    if (bitmap_is_clear_all(&tbl->table->tmp_set))
      continue;
    bitmap_set_all(&tbl->table->tmp_set);
    it.remove();
  }
  return tabs_cnt != mat_derived->elements;
}


/**
  @brief
    Collect fields used in GROUP BY

  @param
    sl           current select
    mat_derived  list of materialized derived tables and views
    gb_items     list of GROUP BY non-field items

  @details
    For each table used in the FROM clause of the SELECT sl collect
    its fields used in the GROUP BY of sl.
    Mark them in tmp_set map.
    If GROUP BY item is not a field store it in gb_items list.

  @retval
    true   if an error occurs
    false  otherwise
*/

bool collect_gb_fields(st_select_lex *sl, List<TABLE_LIST> *mat_derived,
                       List<Item> &gb_items)
{
  THD *thd= sl->join->thd;
  if (!sl->group_list.elements)
    return false;

  for (ORDER *ord= sl->group_list.first; ord; ord= ord->next)
  {
    Item *ord_item= *ord->item;
    if (ord_item->type() == Item::FIELD_ITEM ||
        (ord_item->type() == Item::REF_ITEM &&
        ord_item->real_item()->type() == Item::FIELD_ITEM))
    {
      Item_field *real_it= (Item_field *)(ord_item->real_item());
      bitmap_set_bit(&real_it->field->table->tmp_set,
                     real_it->field->field_index);
    }
    else if (gb_items.push_back(ord_item, thd->mem_root))
      return true;
  }

  /*
    Check if fields used in the GROUP BY are key fields or fields
    of materialized derived tables or views.
  */
  check_allowed_unique_keys(sl);
  check_allowed_materialized_derived(mat_derived);
  return false;
}


/**
  Set subqueries places in the SELECT sl.
  Place here: where this subquery is used (in SELECT list, WHERE or
  HAVING clause of sl).
*/

static
void set_subqueries_context(st_select_lex *sl)
{
  List_iterator_fast<Item> it(sl->item_list);
  Item *item;

  enum_parsing_place ctx= SELECT_LIST;
  while ((item= it++))
  {
    if (item->with_subquery())
      item->walk(&Item::set_subquery_ctx, 0, &ctx);
  }

  Item *cond= sl->join->conds;
  if (cond && cond->with_subquery())
  {
    ctx= IN_WHERE;
    cond->walk(&Item::set_subquery_ctx, 0, &ctx);
  }

  Item *having= sl->join->having;
  if (having && having->with_subquery())
  {
    ctx= IN_HAVING;
    having->walk(&Item::set_subquery_ctx, 0, &ctx);
  }
}


/**
  Check if SELECT list items consists of constants and/or
  'allowed' fields only.
*/

bool are_select_list_fields_allowed(st_select_lex *sl,
                                    List<Item> *gb_items)
{
  Item *item;
  List_iterator<Item> li(sl->item_list);
  while ((item=li++))
  {
    Item *item_arg= NULL;;
    if (item->excl_func_dep_on_grouping_fields(sl, gb_items,
                                               &item_arg))
      continue;
    my_error(ER_NON_GROUPING_FIELD_USED, MYF(0),
             item_arg->real_item()->full_name(), "SELECT list");
    return false;
  }
  return true;
}


/**
  Check if HAVING items consists of constants and/or
  'allowed' fields only.
*/

static
bool are_having_fields_allowed(st_select_lex *sl,
                               Item *having,
                               List<Item> *gb_items)
{
  if (!having)
    return true;

  Item *item_arg= NULL;
  if (having->excl_func_dep_on_grouping_fields(sl, gb_items, &item_arg))
    return true;
  my_error(ER_NON_GROUPING_FIELD_USED, MYF(0),
           item_arg->real_item()->full_name(), "HAVING clause");
  return false;
}


/**
  @brief
    Mark non-'allowed' field as 'allowed' if possible.

  @param
    eq       equality
    dp_part  equality part that depends on 'allowed' field(s) and/or
             constant(s) only
    nd_part  another equality part that should be a single non-'allowed'
             field

  @details
    If non-'allowed' field nd_item is equal through equality eq
    to some function (it can be identity function) of 'allowed'
    field(s) and/or constant(s) dp_item then nd_item is also 'allowed'.
    In other words, non-'allowed' field is functionally dependent
    on 'allowed' field(s) or is constant.

  @note
    dp_part should have the same comparison type as the equality eq.
    It should be so to avoid conversion of dp_part to eq type.
    Otherwise conversion can transform dp_part to function
    that can't be used for extraction of new functional dependent field
    anymore.

  @retval
    true   if non-'allowed' field is marked as 'allowed'
    false  otherwise
*/

static
bool extract_new_func_dep_field(Item_func_eq *eq, Item *dp_part, Item *nd_part)
{
  if (nd_part->real_item()->type() != Item::FIELD_ITEM ||
      (dp_part->type_handler_for_comparison() !=
       eq->compare_type_handler()))
    return false;

  Field *fld= ((Item_field *)nd_part)->field;
  if (bitmap_is_set(&fld->table->tmp_set, fld->field_index))
    return false;
  /* Mark nd_item field as 'allowed' */
  bitmap_set_bit(&fld->table->tmp_set, fld->field_index);
  /* If field is a materialized derived table field */
  if (fld->table->pos_in_table_list->is_materialized_derived())
    bitmap_set_all(&fld->table->tmp_set);
  return true;
}


/**
  @brief
    Check if new 'allowed' field can be extracted from the equality

  @param
    eq_item   equality that needs to be checked
    sl        current select
    eq_items  list of WHERE clause equalities fields information

  @details
    Divide equality into two parts (left and right) and check if left
    and right parts are functionally dependent on 'allowed' fields.

    There can be several cases:

    1. Both parts of the equality depend on 'allowed' fields only.
       Then no new 'allowed' field can be extracted from this equality.
    2. Both parts of the equality don't depend on 'allowed' fields only.
         a. There is a chance that after processing some other equality
            new 'allowed' field will be extracted. This field will make
            left or right part dependent on 'allowed' fields only.
            If so, new 'allowed' field can be extracted from the other
            part of the equality.
         b. Information about this equality (left and right parts fields)
            is saved in eq_items list and is used in future processing.
    3. One part (let it be left part) depends on allowed fields only and
       the other part (right) depends on non-'allowed' fields only.
       Then extract_new_func_dep_field() method is called to check
       if a new 'allowed' field can be extracted from the right part of
       the equality.

  @retval
    true   if an error occurs
    false  otherwise
*/

static bool check_equality_on_new_func_dep(Item_func_eq *eq_item,
                                           st_select_lex *sl,
                                           List<Item_equal_fd_info> &eq_items)
{
  THD *thd= sl->join->thd;
  Item *item_arg= 0;

  Item *item_l= eq_item->arguments()[0];  /* Left part of the equality */
  Item *item_r= eq_item->arguments()[1];  /* Right part of the equality */
  List<Field> fields_l;  /* Fields used in the left part of the equality */
  List<Field> fields_r;  /* Fields used in the right part of the equality */

  if ((item_l->type() == Item::FUNC_ITEM &&
      !((Item_func *)item_l)->is_deterministic) ||
      (item_r->type() == Item::FUNC_ITEM &&
      !((Item_func *)item_r)->is_deterministic))
    return false;

  bool dep_l=
    item_l->excl_func_dep_from_equalities(sl, &item_arg, &fields_l);

  /*
    Left part of the equality contains:
    1. Items that can't be used for extraction of new functionally dependent
       field
    or
    2. Field that can't be used in the WHERE clause of the SELECT
       where eq_item is used.
  */
  if (!dep_l && fields_l.is_empty())
  {
    /*
      Non-'allowed' field is used in WHERE.
      Example:

      SELECT (                        <------------- sl1
        SELECT inner.a           <----------------- sl2
        FROM t1 AS inner
        WHERE (outer.b > 1)
        GROUP BY inner.a
      ) FROM t1 AS outer
      GROUP BY outer.a;

      Here outer.b can't be used in the WHERE clause of the inner
      SELECT inner. inner is located in the SELECT list of outer where
      it is forbidden to use non-'allowed' outer fields.
    */
    if (item_arg)
    {
      my_error(ER_NON_GROUPING_FIELD_USED, MYF(0),
               item_arg->real_item()->full_name(), "WHERE clause");
      return true;
    }
    return false;
  }

  bool dep_r=
    item_r->excl_func_dep_from_equalities(sl, &item_arg, &fields_r);

  /*
    1. Both parts of the equality depend on 'allowed' fields only.
    or
    1'. Right part of the equality contains:
        a. Items that can't be used for extraction of new functionally
           dependent field
        or
        b. Field that can't be used in the WHERE clause of the SELECT
           where eq_item is used.
    or
    1''. Both left and right parts don't contain exactly one field.
         So the equality eq_item is of the form:
         (F11,...,F1n) = (F11,...,F1m)
         From such an equality no new 'allowed' field can be extracted.
  */
  if ((dep_l && dep_r) ||
      (!dep_r && fields_r.is_empty()) ||
      (fields_l.elements != 1 && fields_r.elements != 1))
  {
    if (item_arg)
    {
      my_error(ER_NON_GROUPING_FIELD_USED, MYF(0),
               item_arg->real_item()->full_name(), "WHERE clause");
      return true;
    }
    return false;
  }
  /* 2. Both parts don't depend on 'allowed' fields only. */
  if (!dep_l && !dep_r)
  {
    Item_equal_fd_info *equal_info=
      new (thd->mem_root) Item_equal_fd_info(eq_item, fields_l, fields_r);
    if (eq_items.push_back(equal_info, thd->mem_root))
      return true;
    return false;
  }
  /*
    3. One part depends on allowed fields only and the other part depends
       on non-'allowed' fields only.
  */
  if (dep_l)
    extract_new_func_dep_field(eq_item, item_l, item_r);
  else if (dep_r)
    extract_new_func_dep_field(eq_item, item_r, item_l);
  return false;
}


/**
  @brief
    Get information about fields used in the WHERE clause.

  @param
    sl           current select
    mat_derived  list of materialized derived tables and views

  @details
    This method is divided into several stages:

    1. Traverse WHERE clause and check if it depends on non-'allowed'
       fields of outer SELECTs.

       If WHERE clause is an equality or it is an AND-condition that
       contains some equalities then check_equality_on_new_func_dep() method
       is called to check if some new 'allowed' fields can be extracted
       from these equalities.
       If ‘allowed’ field can’t be extracted from the equality on this step
       this equality information is saved into eq_items list.
    2. If there are no items in eq_items list then no new 'allowed' fields
       can be extracted.

       This can happen because:
       a. There are no equalities in the WHERE clause from which some new
          'allowed' fields can be extracted.
       b. All equalities have already been processed and all possible
          ‘allowed’ fields have been extracted.
    3. If no new 'allowed' fields were extracted on the step 1. then no new
       'allowed' field can be extracted from the eq_items list equalities.
    4. Go through the eq_items list trying to extract new 'allowed' fields.
       Stop if no new 'allowed' fields were extracted on the previous step
       or there are no equalities from which ‘allowed’ fields can be extracted.

  @retval
    true   if an error occurs
    false  otherwise
*/

static
bool check_where_and_get_new_dependencies(st_select_lex *sl,
                                          List<TABLE_LIST> *mat_derived)
{
  Item *cond= sl->join->conds;
  if (!cond)
    return false;

  List<Item_equal_fd_info> eq_items;
  List<Field> fields;
  List<Item> gb_items;
  Item *item_arg= 0;
  uint eq_count= 0;

  /*
    1. Traverse WHERE clause and check if it doesn't depend on non-'allowed'
       fields of outer SELECTs.
  */
  if (cond && cond->type() == Item::COND_ITEM &&
      ((Item_cond*) cond)->functype() == Item_func::COND_AND_FUNC)
  {
    List_iterator_fast<Item> li(*((Item_cond*) cond)->argument_list());
    Item *item;
    while ((item=li++))
    {
      if (item->type() == Item::FUNC_ITEM &&
          ((Item_func_eq *) item)->functype() == Item_func::EQ_FUNC)
      {
        eq_count++;
        if (check_equality_on_new_func_dep((Item_func_eq *)item, sl, eq_items))
          return true;
      }
      else
      {
        if (!item->excl_func_dep_from_equalities(sl, &item_arg, &fields) &&
            item_arg)
        {
          my_error(ER_NON_GROUPING_FIELD_USED, MYF(0),
                   item_arg->real_item()->full_name(), "WHERE clause");
          return true;
        }
      }
    }
  }
  else if (cond->type() == Item::FUNC_ITEM &&
           ((Item_func*) cond)->functype() == Item_func::EQ_FUNC)
  {
    eq_count++;
    if (check_equality_on_new_func_dep((Item_func_eq *)cond, sl, eq_items))
      return true;
  }
  else
  {
    if (!cond->excl_func_dep_from_equalities(sl, &item_arg, &fields) &&
           item_arg)
    {
      my_error(ER_NON_GROUPING_FIELD_USED, MYF(0),
               item_arg->real_item()->full_name(), "WHERE clause");
      return true;
    }
  }
  /*
    2. If there are no items in eq_items list then no new 'allowed' fields
       can be extracted.
  */
  if (eq_items.is_empty())
  {
    check_allowed_unique_keys(sl);
    return false;
  }
  /*
    3. If no new 'allowed' fields were extracted on the step 1. then no new
       'allowed' field can be extracted from the eq_items list equalities.
  */
  if (eq_count == eq_items.elements)
    return false;

  List_iterator<Item_equal_fd_info> li(eq_items);
  Item_equal_fd_info *eq_it;
  bool extracted= true;

  /*
    4. Go through the eq_items list trying to extract new 'allowed' fields.
  */
  while (extracted && !eq_items.is_empty())
  {
    extracted= false;
    li.rewind();
    while ((eq_it= li++))
    {
      List_iterator_fast<Field> it_l(eq_it->fields_l);
      List_iterator_fast<Field> it_r(eq_it->fields_r);
      Field *fld;
      bool dep_l= true;
      bool dep_r= true;

      /*
        Check if left or right (or both) equality part becomes dependent
        on 'allowed' fields only.
      */
      while ((fld= it_l++))
        dep_l&= fld->excl_func_dep_on_grouping_fields(sl, &gb_items, &item_arg);
      while ((fld= it_r++))
        dep_r&= fld->excl_func_dep_on_grouping_fields(sl, &gb_items, &item_arg);

      if (!dep_l && !dep_r)
        continue;
      if (!(dep_l && dep_r) &&
          ((dep_l && extract_new_func_dep_field(eq_it->equal,
                                                eq_it->equal->arguments()[0],
                                                eq_it->equal->arguments()[1])) ||
          (dep_r && extract_new_func_dep_field(eq_it->equal,
                                               eq_it->equal->arguments()[1],
                                               eq_it->equal->arguments()[0]))))
        extracted= true;
      li.remove();
    }
    if (!extracted || eq_items.is_empty())
    {
      /* Check if any keys fields become 'allowed'. */
      if (check_allowed_unique_keys(sl))
        extracted= true;
    }
  }
  return false;
}


/**
  If UPDATE query is used mark all fields of the updated table as 'allowed'.
*/

void set_update_table_fields(st_select_lex *sl)
{
  if (!sl->master_unit()->item ||
      !sl->master_unit()->outer_select() ||
      sl->master_unit()->outer_select()->join)
    return;
  List_iterator<TABLE_LIST> it(sl->master_unit()->outer_select()->leaf_tables);
  TABLE_LIST *tbl;
  while ((tbl= it++))
    bitmap_set_all(&tbl->table->tmp_set);
}


/**
  @brief
    Check if SELECT returns deterministic result.

  @details
    Check if SELECT list and HAVING clause of this SELECT depend on 'allowed'
    fields only.
    'Allowed' fields list is formed this way:
    a. GROUP BY fields
    b. Fields that are functionally dependent on GROUP BY fields
       (extracted from the WHERE clause equalities).
    c. Fields that are functionally dependent on fields, got from 'b.'
       and 'c.' (also extracted from WHERE clause equalities).

  @note
    If this SELECT is a subquery and it contains outer references
    on parent SELECTs tables, check that all these references
    are also 'allowed'. Fields of SELECT list, HAVING clause and
    WHERE clause are checked.

  @retval
    true   if an error occurs
    false  otherwise
*/

bool st_select_lex::check_func_dep()
{
  THD *thd= join->thd;
  /* Stop if no tables are used or fake SELECT is processed. */
  if (leaf_tables.is_empty() ||
      select_number == UINT_MAX ||
      select_number == INT_MAX)
    return false;

  bool need_check= (group_list.elements > 0) ||
                    (master_unit()->outer_select() &&
                     master_unit()->outer_select()->join) ||
                     having;

  List<TABLE_LIST> mat_derived;
  List_iterator<TABLE_LIST> it(leaf_tables);
  TABLE_LIST *tbl;
  while ((tbl= it++))
  {
    if (!tbl->table)
      continue;
    bitmap_clear_all(&tbl->table->tmp_set);
    if (tbl->is_materialized_derived())
    {
      /*
        Collect materialized derived tables used in the FROM clause
        of this SELECT.
      */
      if (mat_derived.push_back(tbl, thd->mem_root))
        return true;
      continue;
    }
  }
  set_update_table_fields(this); /* UPDATE query processing. */
  set_subqueries_context(this); /* Set subqueries places in this SELECT. */

  if (group_list.elements == 0 && !having)
  {
    /*
      This SELECT has no GROUP BY clause and HAVING.
      If so all FROM clause tables fields are 'allowed'.
    */
    List_iterator<TABLE_LIST> it(leaf_tables);
    TABLE_LIST *tbl;

    while ((tbl= it++))
      bitmap_set_all(&tbl->table->tmp_set);
    if (!need_check)
      return false;
  }

  List<Item> gb_items;
  /* Collect fields from GROUP BY. */
  if (collect_gb_fields(this, &mat_derived, gb_items))
    return true;

  /*
    Try to find new fields that are functionally dependent on 'allowed'
    fields and check if WHERE depends on 'allowed' fields only.
  */
  if (check_where_and_get_new_dependencies(this, &mat_derived))
    return true;
  /*
    Check if SELECT list and HAVING clause depend on 'allowed' fields only.
  */
  if (!are_select_list_fields_allowed(this, &gb_items) ||
      !are_having_fields_allowed(this, join->having, &gb_items))
    return true;

  return false;
}
