
#if !defined(__JSON_CONVERTOR_H__)
#define __JSON_CONVERTOR_H__

#include <stack>
#include <list>
#include <map>
#include "parquet/parquet.h"

namespace parquet_cpp {

namespace json {

using boost::shared_ptr;
using std::ostream;
using std::string;
using std::list;
using std::map;
using std::stack;

enum node_type_t {
  UNKNOWN, PRIMITIVE, LIST, MAP
};

struct node_t;
typedef shared_ptr<node_t> node_hdl_t;
bool is_null_hdl(node_hdl_t v);

struct node_t{
  node_type_t type_;

  string primitive_;
  list<node_hdl_t> seq_;
  map<string, node_hdl_t> map_;

  node_t() : type_(UNKNOWN) {}
  node_t(node_type_t t) : type_(t) {}
  node_t(const string& v) : type_(PRIMITIVE), primitive_(v) {}

  node_t& append(node_hdl_t v);
  node_t& put(const string& k, node_hdl_t v);

  bool is_null() const { return type_ == UNKNOWN;}
  node_hdl_t get(const string& k) const;

  void dump(ostream& oss, int indent = -1) const;

  static node_hdl_t make_node();
  static node_hdl_t make_node(const string& v);
  static node_hdl_t make_map();
  static node_hdl_t make_list();
};

class JsonConvertor : public RecordConvertor {
public:
  JsonConvertor(ostream& oss, SchemaHelper& helper, vector<ValueBatch*>& values,
    bool pretty = false)
  : pretty_(pretty), oss_(oss), helper_(helper), values_(values) {}

  void startRecord();
  void startGroup(int fid);
  void convertField(int fid, int idx);
  void endGroup();
  void endRecord();

protected:
  virtual void format_field_value(ostream& oss, int fid, int idx);

private:
  stack<int> id_stack_;
  stack<node_hdl_t> tree_;

  bool pretty_;
  ostream& oss_;

  SchemaHelper& helper_;
  vector<ValueBatch*>& values_;
};

}
}

#endif //__JSON_CONVERTOR_H__

