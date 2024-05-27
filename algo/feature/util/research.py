
_primitives = (bool, str, int, float, type(None))

def _is_primitive(obj):
    return isinstance(obj, _primitives)


def _param_as_label(param):
    if _is_primitive(param):
        return str(param)
    # new directory is used to avoid the file name limit (256) violation.
    d = {k: v for k, v in vars(param).items()}
    # for backward compatibility
    if 'filter_out_non_gemini_symbol' in d and not d['filter_out_non_gemini_symbol']:
        del d['filter_out_non_gemini_symbol']
    if 'filter_out_reportable_symbols' in d and not d['filter_out_reportable_symbols']:
        del d['filter_out_reportable_symbols']
    return '/'.join([f'{k}({_param_as_label(v)})' for k, v in d.items()])


def get_param_label_for_caching(param, label_prefix, label_suffix=None) -> str:
    raw_label = _param_as_label(param)
    label_tokens = raw_label.split('/')
    label_dirs = []
    label_dir = ''
    for label_token in label_tokens:
        label_dir += f'_{label_token}'
        if len(label_dir) > 200:
            label_dirs.append(label_dir[1:])
            label_dir = ''

    if len(label_dir) > 1:
        label_dirs.append(label_dir[1:])

    label = '/'.join(label_dirs)
    ret = f"{label_prefix}_{label}"
    if label_suffix is not None:
        ret = f"{ret}_{label_suffix}"
    return ret

