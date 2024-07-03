using Luban.DataLoader.Builtin.DataVisitors;
using Luban.Datas;
using Luban.Defs;
using Luban.Types;
using Luban.Utils;

namespace Luban.DataLoader.Builtin.Excel;

[DataLoader("xls")]
[DataLoader("xlsx")]
[DataLoader("xlsm")]
[DataLoader("xlm")]
[DataLoader("csv")]
public class ExcelRowColumnDataSource : DataLoaderBase
{
    private static readonly NLog.Logger s_logger = NLog.LogManager.GetCurrentClassLogger();

    private readonly List<RowColumnSheet> _sheets = new();
    private readonly List<List<Cell>> _notDataCells = new();
    private readonly List<List<Cell>> _dataCells = new();


    public override void Load(string rawUrl, string sheetName, Stream stream)
    {
        s_logger.Trace("{} {}", rawUrl, sheetName);
        RawUrl = rawUrl;
        var hasAddNotData = false;
        foreach (RawSheet rawSheet in SheetLoadUtil.LoadRawSheets(rawUrl, sheetName, stream))
        {
            var sheet = new RowColumnSheet(rawUrl, sheetName, rawSheet.SheetName);
            sheet.Load(rawSheet);
            _sheets.Add(sheet);
            if (!hasAddNotData)
            {
                _notDataCells.AddRange(rawSheet.NotDataCells);
                hasAddNotData = true;
            }
            _dataCells.AddRange(rawSheet.Cells);
        }

        if (_sheets.Count == 0)
        {
            if (!string.IsNullOrWhiteSpace(sheetName))
            {
                throw new Exception($"excel:‘{rawUrl}’ sheet:‘{sheetName}’ 不存在或者不是有效的单元簿(有效单元薄的A0单元格必须是##)");
            }
            else
            {
                throw new Exception($"excel: ‘{rawUrl}’ 不包含有效的单元薄(有效单元薄的A0单元格必须是##).");
            }
        }
    }

    public void Load(params RawSheet[] rawSheets)
    {
        foreach (RawSheet rawSheet in rawSheets)
        {
            var sheet = new RowColumnSheet("__intern__", rawSheet.TableName, rawSheet.SheetName);
            sheet.Load(rawSheet);
            _sheets.Add(sheet);
        }
    }

    public RawSheetTableDefInfo LoadTableDefInfo(string rawUrl, string sheetName, Stream stream)
    {
        return SheetLoadUtil.LoadSheetTableDefInfo(rawUrl, sheetName, stream);
    }

    public override List<Record> ReadMulti(TBean type)
    {
        var datas = new List<Record>();
        foreach (var sheet in _sheets)
        {
            try
            {
                foreach (var r in sheet.GetRows())
                {
                    TitleRow row = r.Row;
                    string tagStr = r.Tag;
                    if (DataUtil.IsIgnoreTag(tagStr))
                    {
                        continue;
                    }
                    var data = (DBean)type.Apply(SheetDataCreator.Ins, sheet, row);
                    datas.Add(new Record(data, sheet.UrlWithParams, DataUtil.ParseTags(tagStr)));
                }
            }
            catch (DataCreateException dce)
            {
                dce.OriginDataLocation = sheet.UrlWithParams;
                throw;
            }
            catch (Exception e)
            {
                throw new Exception($"sheet:{sheet.Name}", e);
            }
        }
        return datas;
    }

    public override Record ReadOne(TBean type)
    {
        throw new Exception($"excel不支持单例读取模式");
    }

    public override List<List<string>> GetDefineRows()
    {
        var result = new List<List<string>>();
        var first = new List<string>() { "", "", "" };
        var hasId = _notDataCells[0][2].Value.ToString().StartsWith("#");
        var hasComment = _notDataCells[0][3].Value.ToString().StartsWith("#");
        foreach (var cells in _notDataCells)
        {
            var cf = cells[0].Value.ToString();
            if (cf == "##type")
                first[0] = cells[1].Value.ToString().Trim();
            else if (cf == "##desc" || cf == "##comment" || cf == "##")
                first[2] = cells[1].Value.ToString();
        }
        result.Add(first);
        foreach (var cells in _dataCells)
        {
            var item = new List<string>();
            item.Add(cells[1].Value.ToString().Trim());
            item.Add(hasId ? cells[2].Value.ToString().Trim() : (result.Count - 1).ToString());
            item.Add(hasComment ? cells[3].Value.ToString() : cells[1].Value.ToString().Trim());
            result.Add(item);
        }
        return result;
    }
}
