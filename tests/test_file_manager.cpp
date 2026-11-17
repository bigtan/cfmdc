#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#ifdef CFMDC_ENABLE_PARQUET
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#endif

#include "cfmdc/utils/Error.h"
#include "cfmdc/utils/FileManager.h"
#include "cfmdc/utils/Helpers.h"

using namespace cfmdc;
using namespace std::chrono_literals;

namespace
{

class TempDir
{
  public:
    explicit TempDir(std::string_view prefix = "cfmdc_test")
    {
        static std::atomic<uint64_t> seq{0};
        const auto tick = std::chrono::steady_clock::now().time_since_epoch().count();
        const auto id = seq.fetch_add(1, std::memory_order_relaxed);
        path_ = std::filesystem::temp_directory_path() /
                (std::string(prefix) + "_" + std::to_string(tick) + "_" + std::to_string(id));
        std::filesystem::create_directories(path_);
    }

    ~TempDir()
    {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    TempDir(const TempDir &) = delete;
    TempDir &operator=(const TempDir &) = delete;

    const std::filesystem::path &path() const noexcept
    {
        return path_;
    }

  private:
    std::filesystem::path path_;
};

MarketTick make_tick(std::string_view instrument_id = "rb2505", std::string_view update_time = "09:30:00")
{
    MarketTick tick{};
    safe_strcpy(tick.trading_day, "20250102");
    safe_strcpy(tick.action_day, "20250102");
    safe_strcpy(tick.exchange_id, "SHFE");
    safe_strcpy(tick.instrument_id, instrument_id);
    safe_strcpy(tick.update_time, update_time);
    tick.last_price = 3500.0;
    return tick;
}

CThostFtdcDepthMarketDataField make_raw_ctp_tick(std::string_view instrument_id = "rb2505",
                                                  std::string_view update_time = "09:30:00")
{
    CThostFtdcDepthMarketDataField raw{};
    safe_strcpy(raw.TradingDay, "20250102");
    safe_strcpy(raw.ActionDay, "20250102");
    safe_strcpy(raw.ExchangeID, "SHFE");
    safe_strcpy(raw.InstrumentID, instrument_id);
    safe_strcpy(raw.UpdateTime, update_time);
    raw.LastPrice = 3500.0;
    return raw;
}

std::vector<std::string> split_csv_line(const std::string &line)
{
    std::vector<std::string> fields;
    size_t start = 0;
    while (true)
    {
        const size_t comma = line.find(',', start);
        if (comma == std::string::npos)
        {
            fields.push_back(line.substr(start));
            break;
        }
        fields.push_back(line.substr(start, comma - start));
        start = comma + 1;
    }
    return fields;
}

std::vector<std::string> read_lines(const std::filesystem::path &file_path)
{
    std::ifstream ifs(file_path);
    REQUIRE(ifs.is_open());
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(ifs, line))
    {
        if (!line.empty() && line.back() == '\r')
        {
            line.pop_back();
        }
        lines.push_back(std::move(line));
    }
    return lines;
}

} // namespace

TEST_CASE("AsyncFileManager marks storage failures as fatal", "[file_manager][failure]")
{
    TempDir tmp("cfmdc_failure");
    const auto invalid_directory = tmp.path() / "storage_path_is_a_file.tmp";
    {
        std::ofstream marker(invalid_directory);
        REQUIRE(marker.is_open());
    }

    auto manager =
        std::make_unique<AsyncFileManager>(invalid_directory, std::filesystem::path{}, "20250102", "20250101",
                                           "20250102", "09:00:00", StorageMode::CSV, AsyncFileManager::Options{});
    REQUIRE(manager->write_market_data_async(make_tick()));

    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!manager->has_fatal_error() && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(1ms);
    }

    REQUIRE(manager->has_fatal_error());
    const auto stats = manager->get_statistics();
    REQUIRE(stats.total_records == 0);
    REQUIRE(stats.write_failures >= 1);
}

TEST_CASE("AsyncFileManager periodically flushes CSV during sustained writes", "[file_manager][flush]")
{
    TempDir tmp("cfmdc_flush");
    const auto output_directory = tmp.path() / "csv";

    AsyncFileManager::Options options;
    options.csv_flush_interval = 1ms;
    auto manager = std::make_unique<AsyncFileManager>(output_directory, std::filesystem::path{}, "20250102", "20250101",
                                                      "20250102", "09:00:00", StorageMode::CSV, options);

    constexpr size_t kRecordCount = 20000;
    const auto tick = make_tick();
    for (size_t i = 0; i < kRecordCount; ++i)
    {
        while (!manager->write_market_data_async(tick))
        {
            REQUIRE_FALSE(manager->has_fatal_error());
            std::this_thread::yield();
        }
    }

    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (manager->get_statistics().total_records < kRecordCount && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(1ms);
    }

    manager->stop();
    const auto stats = manager->get_statistics();
    REQUIRE(stats.total_records == kRecordCount);
    REQUIRE(stats.write_failures == 0);
    REQUIRE(stats.periodic_flushes > 0);
}

TEST_CASE("CsvWriter tracks and clears dirty state per instrument", "[csv_writer][flush]")
{
    TempDir tmp("cfmdc_csv_dirty");
    const auto output_directory = tmp.path() / "csv";

    CsvWriter writer(output_directory, "20250102");
    auto rb_tick = make_tick("rb2505");
    auto ag_tick = make_tick("ag2506");

    REQUIRE(writer.write(rb_tick));
    REQUIRE(writer.write(ag_tick));
    REQUIRE(writer.file_count() == 2);
    REQUIRE(writer.dirty_file_count() == 2);

    REQUIRE(writer.flush());
    REQUIRE(writer.dirty_file_count() == 0);

    // Writing to only one instrument marks only that stream dirty
    REQUIRE(writer.write(rb_tick));
    REQUIRE(writer.dirty_file_count() == 1);

    REQUIRE(writer.flush());
    REQUIRE(writer.dirty_file_count() == 0);
    REQUIRE(writer.close_all());
}

TEST_CASE("CsvWriter validates trading day and writes exact 43-column formatting", "[csv_writer][format]")
{
    TempDir tmp("cfmdc_csv_format");
    const auto output_directory = tmp.path() / "csv" / "";

    REQUIRE_THROWS_AS(CsvWriter(output_directory, "invalid"), FileException);
    REQUIRE_THROWS_AS(CsvWriter(output_directory, "2025010"), FileException);

    {
        CsvWriter writer(output_directory, "20250102");
        auto tick = make_tick("rb2505", "09:30:00");
        tick.last_price = INVALID_PRICE;
        tick.pre_settlement_price = -INVALID_PRICE;
        tick.pre_close_price = std::numeric_limits<double>::quiet_NaN();
        tick.pre_open_interest = std::numeric_limits<double>::infinity();
        tick.open_price = 0.0;
        tick.turnover = 123456.78;
        tick.open_interest = 0.0;
        tick.bid_price = {0.0, -12.5, INVALID_PRICE, 3498.0, 3497.5};
        tick.ask_price = {3501.25, INVALID_PRICE, 3503.0, 3504.0, 3505.0};
        tick.volume = 42;
        tick.update_millisec = 500;
        tick.average_price = 3500.125;

        REQUIRE(writer.write(tick));
        REQUIRE(writer.close_all());
    }

    const auto lines = read_lines(tmp.path() / "csv" / "rb2505_20250102.csv");
    REQUIRE(lines.size() == 2);

    const auto header_cols = split_csv_line(lines[0]);
    REQUIRE(header_cols.size() == 43);
    REQUIRE(header_cols[0] == "TradingDay");
    REQUIRE(header_cols[1] == "InstrumentID");
    REQUIRE(header_cols[11] == "Turnover");
    REQUIRE(header_cols[42] == "ActionDay");

    const auto row_cols = split_csv_line(lines[1]);
    REQUIRE(row_cols.size() == 43);
    REQUIRE(row_cols[0] == "20250102");
    REQUIRE(row_cols[1] == "rb2505");
    REQUIRE(row_cols[2] == "SHFE");
    REQUIRE(row_cols[3].empty());  // LastPrice = DBL_MAX -> empty
    REQUIRE(row_cols[4].empty());  // PreSettlementPrice = -DBL_MAX -> empty
    REQUIRE(row_cols[5].empty());  // PreClosePrice = NaN -> empty
    REQUIRE(row_cols[6].empty());  // PreOpenInterest = +Inf -> empty
    REQUIRE(row_cols[7] == "0");   // OpenPrice = 0.0 preserved
    REQUIRE(row_cols[10] == "42"); // Volume
    REQUIRE(row_cols[11] == "123456.78");
    REQUIRE(row_cols[12] == "0"); // OpenInterest = 0.0 preserved
    REQUIRE(row_cols[19] == "09:30:00");
    REQUIRE(row_cols[20] == "500");
    REQUIRE(row_cols[21] == "0");       // BidPrice1 = 0.0 preserved
    REQUIRE(row_cols[25] == "-12.5");   // BidPrice2 negative spread preserved
    REQUIRE(row_cols[29].empty());      // BidPrice3 = DBL_MAX -> empty
    REQUIRE(row_cols[23] == "3501.25"); // AskPrice1
    REQUIRE(row_cols[27].empty());      // AskPrice2 = DBL_MAX -> empty
    REQUIRE(row_cols[41] == "3500.125");
    REQUIRE(row_cols[42] == "20250102");
}

TEST_CASE("CsvWriter and AsyncFileManager block path traversal and CSV injection", "[file_manager][security]")
{
    TempDir tmp("cfmdc_security");
    const auto csv_dir = tmp.path() / "csv";

    {
        CsvWriter writer(csv_dir, "20250102");
        REQUIRE_FALSE(writer.write(make_tick("../../evil")));
        REQUIRE_FALSE(writer.write(make_tick("=CMD|' /C calc'!A0")));
        REQUIRE_FALSE(writer.write(make_tick("+1234")));
        REQUIRE_FALSE(writer.write(make_tick("sub/rb2505")));
        REQUIRE(writer.file_count() == 0);
    }

    auto manager = std::make_unique<AsyncFileManager>(csv_dir, std::filesystem::path{}, "20250102", "20250101",
                                                      "20250102", "09:00:00", StorageMode::CSV,
                                                      AsyncFileManager::Options{});

    REQUIRE(manager->write_market_data_async(make_tick("../../evil", "09:30:00")));
    REQUIRE(manager->write_market_data_async(make_tick("=HYPERLINK(\"http://x\")", "09:30:00")));
    REQUIRE(manager->write_market_data_async(make_tick("rb2505", "09:30:00")));
    REQUIRE(manager->write_market_data_async(make_raw_ctp_tick("rb2505", "09:30:01")));

    manager->stop();
    REQUIRE(manager->flush_all());
    REQUIRE(manager->close_all());

    const auto stats = manager->get_statistics();
    REQUIRE_FALSE(manager->has_fatal_error());
    REQUIRE(stats.write_failures == 0);
    REQUIRE(stats.invalid_records == 2);
    REQUIRE(stats.filtered_records == 0);
    REQUIRE(stats.total_records == 2);

    REQUIRE_FALSE(std::filesystem::exists(tmp.path() / "evil_20250102.csv"));
    REQUIRE(std::filesystem::exists(csv_dir / "rb2505_20250102.csv"));
}

TEST_CASE("AsyncFileManager tracks filtered_records for out-of-window UpdateTime", "[file_manager][filter]")
{
    TempDir tmp("cfmdc_filter");
    const auto csv_dir = tmp.path() / "csv";

    auto manager = std::make_unique<AsyncFileManager>(csv_dir, std::filesystem::path{}, "20250102", "20250101",
                                                      "20250102", "09:00:00", StorageMode::CSV,
                                                      AsyncFileManager::Options{});

    REQUIRE(manager->write_market_data_async(make_tick("rb2505", "16:00:00"))); // outside Day window
    REQUIRE(manager->write_market_data_async(make_tick("rb2505", "99:99:99"))); // invalid HH:MM:SS
    REQUIRE(manager->write_market_data_async(make_tick("rb2505", "09:30:00"))); // inside Day window

    manager->stop();
    REQUIRE(manager->flush_all());
    REQUIRE(manager->close_all());

    const auto stats = manager->get_statistics();
    REQUIRE_FALSE(manager->has_fatal_error());
    REQUIRE(stats.filtered_records == 2);
    REQUIRE(stats.invalid_records == 0);
    REQUIRE(stats.total_records == 1);
}

#ifdef CFMDC_ENABLE_PARQUET
TEST_CASE("AsyncFileManager drains CSV and Parquet workers independently", "[file_manager][hybrid]")
{
    TempDir tmp("cfmdc_hybrid");
    const auto output_root = tmp.path();

    AsyncFileManager::Options options;
    options.parquet_row_group_size = 256;
    auto manager = std::make_unique<AsyncFileManager>(output_root / "csv", output_root / "parquet", "20250102",
                                                      "20250101", "20250102", "09:00:00", StorageMode::HYBRID, options);

    constexpr size_t kRecordCount = 2000;
    const auto tick = make_tick();
    for (size_t i = 0; i < kRecordCount; ++i)
    {
        while (!manager->write_market_data_async(tick))
        {
            REQUIRE_FALSE(manager->has_fatal_error());
            std::this_thread::yield();
        }
    }

    manager->stop();
    REQUIRE(manager->flush_all());
    REQUIRE(manager->close_all());
    const auto stats = manager->get_statistics();
    REQUIRE(stats.total_records == kRecordCount);
    REQUIRE(stats.csv_records == kRecordCount);
    REQUIRE(stats.parquet_records == kRecordCount);
    REQUIRE(stats.queue_size == 0);
    REQUIRE(stats.parquet_queue_size == 0);
    REQUIRE(stats.write_failures == 0);
}

TEST_CASE("ParquetBatchWriter validates trading day and writes nullable 43-column Arrow table",
          "[file_manager][parquet]")
{
    TempDir tmp("cfmdc_parquet_readback");
    const auto parquet_dir = tmp.path() / "parquet";

    REQUIRE_THROWS_AS(ParquetBatchWriter(parquet_dir, "202501"), FileException);

    {
        AsyncFileManager::Options options;
        options.worker_core = -2;
        options.parquet_worker_core = -2;
        options.parquet_row_group_size = 64;

        auto manager = std::make_unique<AsyncFileManager>(std::filesystem::path{}, parquet_dir, "20250102", "20250101",
                                                          "20250102", "21:00:00", StorageMode::PARQUET, options);

        auto tick0 = make_tick("rb2505", "21:30:00");
        tick0.last_price = INVALID_PRICE;
        tick0.open_price = 0.0;
        tick0.pre_open_interest = std::numeric_limits<double>::infinity();
        tick0.turnover = 123456.78;
        tick0.open_interest = 0.0;
        tick0.bid_price[0] = std::numeric_limits<double>::quiet_NaN();
        tick0.ask_price[0] = 0.0;
        tick0.average_price = INVALID_PRICE;

        auto tick1 = make_tick("ag2506", "01:15:00");
        tick1.last_price = 0.0;
        tick1.open_price = 5200.5;
        tick1.turnover = 987654.321;
        tick1.bid_price[0] = -12.5;
        tick1.ask_price[0] = -INVALID_PRICE;
        tick1.average_price = 0.0;

        REQUIRE(manager->write_market_data_async(tick0));
        REQUIRE(manager->write_market_data_async(tick1));

        manager->stop();
        REQUIRE(manager->flush_all());
        REQUIRE(manager->close_all());

        const auto stats = manager->get_statistics();
        REQUIRE_FALSE(manager->has_fatal_error());
        REQUIRE(stats.parquet_records == 2);
        REQUIRE(stats.total_records == 2);
    }

    std::filesystem::path parquet_file;
    for (const auto &entry : std::filesystem::directory_iterator(parquet_dir))
    {
        if (entry.path().extension() == ".parquet")
        {
            parquet_file = entry.path();
            break;
        }
    }
    REQUIRE_FALSE(parquet_file.empty());

    auto infile_result = arrow::io::ReadableFile::Open(parquet_file.string());
    REQUIRE(infile_result.ok());

    auto reader_result = parquet::arrow::OpenFile(*infile_result, arrow::default_memory_pool());
    REQUIRE(reader_result.ok());

    auto table_result = (*reader_result)->ReadTable();
    REQUIRE(table_result.ok());
    std::shared_ptr<arrow::Table> table = *table_result;
    REQUIRE(table != nullptr);

    REQUIRE(table->num_columns() == 43);
    REQUIRE(table->schema()->field(42)->name() == "ActionDay");
    REQUIRE(table->num_rows() == 2);

    auto trading_day = std::static_pointer_cast<arrow::StringArray>(table->GetColumnByName("TradingDay")->chunk(0));
    auto instrument_id = std::static_pointer_cast<arrow::StringArray>(table->GetColumnByName("InstrumentID")->chunk(0));
    auto action_day = std::static_pointer_cast<arrow::StringArray>(table->GetColumnByName("ActionDay")->chunk(0));
    auto last_price = std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("LastPrice")->chunk(0));
    auto open_price = std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("OpenPrice")->chunk(0));
    auto pre_oi = std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("PreOpenInterest")->chunk(0));
    auto turnover = std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("Turnover")->chunk(0));
    auto open_interest = std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("OpenInterest")->chunk(0));
    auto bid_price1 = std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("BidPrice1")->chunk(0));
    auto ask_price1 = std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("AskPrice1")->chunk(0));
    auto avg_price = std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("AveragePrice")->chunk(0));

    REQUIRE(trading_day->GetString(0) == "20250102");
    REQUIRE(trading_day->GetString(1) == "20250102");
    // Sorted by InstrumentID within the row group: "ag2506" precedes "rb2505".
    REQUIRE(instrument_id->GetString(0) == "ag2506");
    REQUIRE(instrument_id->GetString(1) == "rb2505");
    REQUIRE(action_day->GetString(0) == "20250102"); // 01:15:00 <= 07:00:00 -> next_action_day
    REQUIRE(action_day->GetString(1) == "20250101"); // 21:30:00 -> base_action_day

    REQUIRE_FALSE(last_price->IsNull(0));
    REQUIRE(last_price->Value(0) == 0.0);
    REQUIRE(last_price->IsNull(1));

    REQUIRE_FALSE(open_price->IsNull(0));
    REQUIRE(open_price->Value(0) == 5200.5);
    REQUIRE_FALSE(open_price->IsNull(1));
    REQUIRE(open_price->Value(1) == 0.0);

    REQUIRE(pre_oi->IsNull(1));

    REQUIRE_FALSE(turnover->IsNull(0));
    REQUIRE(turnover->Value(0) == 987654.321);
    REQUIRE_FALSE(turnover->IsNull(1));
    REQUIRE(turnover->Value(1) == 123456.78);

    REQUIRE_FALSE(open_interest->IsNull(1));
    REQUIRE(open_interest->Value(1) == 0.0);

    REQUIRE_FALSE(bid_price1->IsNull(0));
    REQUIRE(bid_price1->Value(0) == -12.5);
    REQUIRE(bid_price1->IsNull(1));

    REQUIRE(ask_price1->IsNull(0));
    REQUIRE_FALSE(ask_price1->IsNull(1));
    REQUIRE(ask_price1->Value(1) == 0.0);

    REQUIRE_FALSE(avg_price->IsNull(0));
    REQUIRE(avg_price->Value(0) == 0.0);
    REQUIRE(avg_price->IsNull(1));
}

TEST_CASE("ParquetMarketDataWriter sorts row group stably by InstrumentID and uses BYTE_STREAM_SPLIT for doubles",
          "[file_manager][parquet][sort]")
{
    TempDir tmp("cfmdc_parquet_sort_bss");
    const auto parquet_dir = tmp.path() / "parquet";

    {
        AsyncFileManager::Options options;
        options.parquet_row_group_size = 64;
        auto manager = std::make_unique<AsyncFileManager>(std::filesystem::path{}, parquet_dir, "20250102", "20250101",
                                                          "20250102", "09:00:00", StorageMode::PARQUET, options);

        auto t1 = make_tick("rb2505", "09:30:00");
        t1.last_price = 3501.0;
        t1.volume = 1;

        auto t2 = make_tick("ag2506", "09:30:00");
        t2.last_price = 7100.0;
        t2.volume = 2;

        auto t3 = make_tick("rb2505", "09:30:01");
        t3.last_price = 3502.0;
        t3.volume = 3;

        auto t4 = make_tick("IF2506", "09:30:01");
        t4.last_price = INVALID_PRICE;
        t4.volume = 4;

        auto t5 = make_tick("ag2506", "09:30:02");
        t5.last_price = 7105.0;
        t5.volume = 5;

        for (const auto &t : {t1, t2, t3, t4, t5})
        {
            REQUIRE(manager->write_market_data_async(t));
        }

        manager->stop();
        REQUIRE(manager->flush_all());
        REQUIRE(manager->close_all());
        REQUIRE_FALSE(manager->has_fatal_error());
    }

    std::filesystem::path parquet_file;
    for (const auto &entry : std::filesystem::directory_iterator(parquet_dir))
    {
        if (entry.path().extension() == ".parquet")
        {
            parquet_file = entry.path();
            break;
        }
    }
    REQUIRE_FALSE(parquet_file.empty());

    auto infile_result = arrow::io::ReadableFile::Open(parquet_file.string());
    REQUIRE(infile_result.ok());
    auto reader_result = parquet::arrow::OpenFile(*infile_result, arrow::default_memory_pool());
    REQUIRE(reader_result.ok());

    auto metadata = (*reader_result)->parquet_reader()->metadata();
    REQUIRE(metadata->num_row_groups() == 1);
    const auto last_price_encodings = metadata->RowGroup(0)->ColumnChunk(3)->encodings();
    REQUIRE(std::find(last_price_encodings.begin(), last_price_encodings.end(),
                      parquet::Encoding::BYTE_STREAM_SPLIT) != last_price_encodings.end());

    auto table_result = (*reader_result)->ReadTable();
    REQUIRE(table_result.ok());
    std::shared_ptr<arrow::Table> table = *table_result;
    REQUIRE(table->num_rows() == 5);

    auto inst = std::static_pointer_cast<arrow::StringArray>(table->GetColumnByName("InstrumentID")->chunk(0));
    auto lp = std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("LastPrice")->chunk(0));
    auto vol = std::static_pointer_cast<arrow::Int32Array>(table->GetColumnByName("Volume")->chunk(0));

    // ASCII lexicographical order: "IF2506" < "ag2506" < "rb2505", with intra-instrument order preserved.
    REQUIRE(inst->GetString(0) == "IF2506");
    REQUIRE(lp->IsNull(0));
    REQUIRE(vol->Value(0) == 4);

    REQUIRE(inst->GetString(1) == "ag2506");
    REQUIRE(lp->Value(1) == 7100.0);
    REQUIRE(vol->Value(1) == 2);

    REQUIRE(inst->GetString(2) == "ag2506");
    REQUIRE(lp->Value(2) == 7105.0);
    REQUIRE(vol->Value(2) == 5);

    REQUIRE(inst->GetString(3) == "rb2505");
    REQUIRE(lp->Value(3) == 3501.0);
    REQUIRE(vol->Value(3) == 1);

    REQUIRE(inst->GetString(4) == "rb2505");
    REQUIRE(lp->Value(4) == 3502.0);
    REQUIRE(vol->Value(4) == 3);
}
#endif
