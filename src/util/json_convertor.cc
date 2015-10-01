
#include "util/json_convertor.h"

using namespace parquet;
using namespace std;

namespace parquet_cpp {
namespace json {

node_hdl_t node_t::make_node() {
  return node_hdl_t(new node_t());
}

node_hdl_t node_t::make_node(const string& v) {
  return node_hdl_t(new node_t(v));
}

node_hdl_t node_t::make_list() {
  return node_hdl_t(new node_t(LIST));
}
node_hdl_t node_t::make_map() {
  return node_hdl_t(new node_t(MAP));
}

node_t& node_t::append(node_hdl_t v) {
  if (type_ == UNKNOWN || type_ == LIST) {
    type_ = LIST;
    seq_.push_back(v);
  }
  return *this;
}

node_t& node_t::put(const string& k, node_hdl_t v) {
  if (type_ == UNKNOWN || type_ == MAP) {
    type_ = MAP;
    map_[k] = v;
  }
  return *this;
}

node_hdl_t node_t::get(const string& k) const {
  if (type_ == MAP) {
    map<string, node_hdl_t>::const_iterator i = map_.find(k);
    if (i == map_.end())
      return make_node();
    return i->second;
  }
  return make_node();
}

bool is_null_hdl(node_hdl_t v) {
  return (!v || v->is_null());
}

static int _passdown(int v) {
  if (v >= 0) return (1 + v);
  return v;
}

static void _do_wrap(ostream& oss, int indent) {
  if (indent < 0) return;
  oss << "\n";
  for (int i=0; i<indent; ++i) oss << "  ";
}

static void _do_indent(ostream& oss, int indent) {
  for (int i=0; i<indent; ++i) oss << "  ";
}

static ostream& escape_str(ostream& oss, const string& v) {
  oss << '"';
  for (int i = 0; i < v.size(); ++i) {
    switch ( v[i] ) {
    case '"': oss << "\\\""; break;
    case '\\': oss << "\\\\"; break;
    default: oss << v[i];
    }
  }
  oss << '"';
  return oss;
}

void node_t::dump(ostream& oss, int indent) const {
  if (type_ == PRIMITIVE) {
    escape_str(oss, primitive_);
  } else if (type_ == LIST) {
    oss << "[";
    list<node_hdl_t>::const_iterator i = seq_.begin();
    if (i != seq_.end()) {
      (*i)->dump(oss, indent);
      for (i++; i != seq_.end(); ++i) {
        oss << ",";
        (*i)->dump(oss, indent);
      }
    }
    oss << "]";
  } else if (type_ == MAP) {
    map<string, node_hdl_t>::const_iterator i = map_.begin();
    oss << "{";
    _do_wrap(oss, _passdown(indent));
    if (i != map_.end()) {
      escape_str(oss, i->first) << " : ";
      i->second->dump(oss, _passdown(indent));
      for (i++; i != map_.end(); ++i) {
        oss << ",";
        _do_wrap(oss, _passdown(indent));
        escape_str(oss, i->first) << " : ";
        i->second->dump(oss, _passdown(indent));
      }
      _do_wrap(oss, indent);
    }
    oss << "}";
  }
}

void JsonConvertor::startRecord() {
  while (!tree_.empty()) tree_.pop();
  tree_.push(node_t::make_map());

  while (!id_stack_.empty()) id_stack_.pop();
  id_stack_.push(0);
}

void JsonConvertor::startGroup(int fid) {
  int pid = id_stack_.top();
  id_stack_.push(fid);
  const ColumnDescriptor& desc = helper_.columns[fid];
  if (desc.element.repetition_type == FieldRepetitionType::REPEATED) {
    node_hdl_t s = tree_.top()->get(desc.element.name);
    if (is_null_hdl(s)) {
      node_hdl_t l = node_t::make_list();
      tree_.top()->put(desc.element.name, l);
      s = node_t::make_node();
      l->append(s);
      tree_.push(s);
    } else {
      node_hdl_t s2 = node_t::make_node();
      s->append(s2);
      tree_.top()->append(s2);
      tree_.push(s2);
    }
  } else {
    node_hdl_t s = node_t::make_map();
    tree_.top()->put(desc.element.name, s);
    tree_.push(s);
  }
}

void JsonConvertor::convertField(int fid, int idx) {
  const ColumnDescriptor& desc = helper_.columns[fid];
  stringstream oss;
  format_field_value(oss, fid, idx);
  if (desc.element.repetition_type == FieldRepetitionType::REPEATED) {
    node_hdl_t s = tree_.top()->get(desc.element.name);
    if (is_null_hdl(s)) {
      s = node_t::make_list();
      tree_.top()->put(desc.element.name, s);
    }
    s->append(node_t::make_node(oss.str()));
  } else {
    tree_.top()->put(desc.element.name, node_t::make_node(oss.str()));
  }
}

void JsonConvertor::endGroup() {
  id_stack_.pop();
  tree_.pop();
}

void JsonConvertor::endRecord() {
  tree_.top()->dump(oss_, pretty_?0:-1);
}

void JsonConvertor::format_field_value(ostream& oss, int fid, int idx) {
  const ColumnDescriptor& desc = helper_.columns[fid];
  ValueBatch& batch = *(values_[fid]);
  if (!batch.isNull(idx)) {
    switch (desc.element.type) {
    case Type::INT32: oss << batch.Get<int32_t>(idx); break;
    case Type::INT64: oss << batch.Get<int64_t>(idx); break;
    case Type::FLOAT: oss << batch.Get<float>(idx); break;
    case Type::DOUBLE: oss << batch.Get<double>(idx); break;
    case Type::BYTE_ARRAY: oss << batch.Get<ByteArray>(idx); break;
    default:
      cerr <<__FILE__<<':'<<__LINE__<<"doesn't support "
           << desc.element.type << " yet\n";
      return;
    }
  }
}

}
}

