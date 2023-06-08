
#include <algorithm>
#include <csignal>
#include <boost/program_options.hpp>
#include <boost/stacktrace.hpp>
#include <boost/exception/all.hpp>
#include <glog/logging.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/support/server_interceptor.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <string>
#include <vector>

#include "boost/move/algorithm.hpp"
#include "boost/range/adaptor/map.hpp"
#include "boost/algorithm/string/join.hpp"
#include "boost/range/end.hpp"
#include "common/Schema.h"
#include "common/Types.h"
#include "grpcpp/resource_quota.h"
#include "log/Log.h"
#include "query/Plan.h"
#include "segcore/Reduce.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentSealed.h"
#include "segcore/SegmentSealedImpl.h"
#include "segcore/segcore_init_c.h"
#include "pb/segcore.grpc.pb.h"
#include "storage/Types.h"
#include "index/IndexFactory.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "segcore/Types.h"
#include "storage/Util.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/LocalChunkManagerSingleton.h"

using SegmentPtr = std::shared_ptr<milvus::segcore::SegmentInterface>;
using traced =
    boost::error_info<struct tag_stacktrace, boost::stacktrace::stacktrace>;

class SegmentManager {
 public:
    explicit SegmentManager(milvus::SchemaPtr schemaPtr)
        : schemaPtr(std::move(schemaPtr)) {
    }

    void
    Add(int64_t segmentId, bool is_sealed = true) {
        // std::lock_guard lock(mutex_);

        LOG_SEGCORE_INFO_ << "add segment " << segmentId;
        if (is_sealed) {
            auto segment =
                milvus::segcore::CreateSealedSegment(schemaPtr, segmentId);
            segments[segmentId] = std::move(segment);
        } else {
            auto segment =
                milvus::segcore::CreateGrowingSegment(schemaPtr, nullptr);
            segments[segmentId] = std::move(segment);
        }
    }

    SegmentPtr&
    Get(int64_t segmentId) {
        // std::lock_guard lock(mutex_);

        if (segments[segmentId] == nullptr) {
            Add(segmentId);
        }
        return segments[segmentId];
    }

    milvus::SchemaPtr&
    GetSchema() {
        return schemaPtr;
    }

 private:
    milvus::SchemaPtr schemaPtr;
    std::unordered_map<int64_t, SegmentPtr> segments{};

    std::shared_mutex mutex_{};
};

class CollectionManager {
 public:
    void
    Add(int64_t collectionId, milvus::SchemaPtr schemaPtr) {
        // std::lock_guard lock(mutex_);
        LOG_SEGCORE_INFO_ << "add collection:" << collectionId;
        auto segmentManager = std::make_shared<SegmentManager>(schemaPtr);
        collections[collectionId] = std::move(segmentManager);
    }

    void
    Remove(int64_t collectionId) {
        // std::lock_guard lock(mutex_);
        collections.erase(collectionId);
    }

    std::shared_ptr<SegmentManager>&
    Get(int64_t collectionId) {
        // std::lock_guard lock(mutex_);
        if (collections[collectionId] == nullptr) {
            Add(collectionId, nullptr);
        }
        return collections[collectionId];
    }

 private:
    std::unordered_map<int64_t, std::shared_ptr<SegmentManager>> collections;
    mutable std::shared_mutex mutex_;
};

class SegcoreService final
    : public milvus::proto::segcore::Segcore::CallbackService {
 public:
    explicit SegcoreService(
        std::shared_ptr<CollectionManager> collectionManager)
        : cm_(std::move(collectionManager)) {
    }

    grpc::ServerUnaryReactor*
    NewCollection(grpc::CallbackServerContext* context,
                  const milvus::proto::segcore::NewCollectionRequest* request,
                  milvus::proto::common::Status* response) override {
        cm_->Add(request->collectionid(),
                 milvus::Schema::ParseFrom(request->schema()));
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

    grpc::ServerUnaryReactor*
    DeleteCollection(
        grpc::CallbackServerContext* context,
        const milvus::proto::segcore::DeleteCollectionRequest* request,
        milvus::proto::common::Status* response) override {
        cm_->Remove(request->collectionid());
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

    grpc::ServerUnaryReactor*
    LoadSegments(grpc::CallbackServerContext* context,
                 const milvus::proto::segcore::LoadSegmentsRequest* request,
                 milvus::proto::common::Status* response) override {
        auto sm = cm_->Get(request->collectionid());
        auto segment = sm->Get(request->segmentid());
        LOG_SEGCORE_INFO_ << "Got Segment: " << segment->get_segment_id();

        LoadFieldDataInfo info;
        for (auto& field : request->fields()) {
            FieldBinlogInfo binlog_info;
            binlog_info.field_id = field.fieldid();
            binlog_info.row_count = request->rowcount();
            for (auto& log : field.binlogs()) {
                binlog_info.insert_files.push_back(log.log_path());
            }
            info.field_infos[field.fieldid()] = binlog_info;
        }
        info.mmap_dir_path = request->mmap_dir_path();
        segment->LoadFieldData(info);
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

    grpc::ServerUnaryReactor*
    UpdateSegmentIndex(
        grpc::CallbackServerContext* context,
        const milvus::proto::segcore::UpdateIndexRequest* request,
        milvus::proto::common::Status* response) override {
        auto sm = cm_->Get(request->collectionid());
        auto segment = static_cast<milvus::segcore::SegmentSealed*>(
            sm->Get(request->segmentid()).get());
        LOG_SEGCORE_INFO_ << "Got Segment: " << segment->get_segment_id();

        milvus::segcore::LoadIndexInfo info;
        info.collection_id = request->collectionid();
        info.partition_id = request->partitionid();
        info.segment_id = request->segmentid();
        info.index_id = request->indexid();
        info.field_id = request->fieldid();
        info.field_type =
            static_cast<milvus::storage::DataType>(request->field_type());

        info.index_files.resize(request->index_files_size());
        std::copy(request->index_files().begin(),
                  request->index_files().end(),
                  info.index_files.begin());
        for (auto& param : request->index_params()) {
            info.index_params[param.key()] = param.value();
        }

        LOG_SEGCORE_INFO_ << "Load index info "
                          << std::to_string(info.segment_id);
        auto& index_params = info.index_params;
        auto field_type = info.field_type;

        milvus::index::CreateIndexInfo index_info;
        index_info.field_type = info.field_type;

        // get index type
        AssertInfo(index_params.find("index_type") != index_params.end(),
                   "index type is empty");
        index_info.index_type = index_params.at("index_type");

        // get metric type
        if (milvus::datatype_is_vector(field_type)) {
            AssertInfo(index_params.find("metric_type") != index_params.end(),
                       "metric type is empty for vector index");
            index_info.metric_type = index_params.at("metric_type");
        }

        // init file manager
        milvus::storage::FieldDataMeta field_meta{info.collection_id,
                                                  info.partition_id,
                                                  info.segment_id,
                                                  info.field_id};
        milvus::storage::IndexMeta index_meta{info.segment_id,
                                              info.field_id,
                                              info.index_build_id,
                                              info.index_version};
        auto remote_chunk_manager =
            milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                .GetRemoteChunkManager();
        auto file_manager =
            milvus::storage::CreateFileManager(index_info.index_type,
                                               field_meta,
                                               index_meta,
                                               remote_chunk_manager);
        AssertInfo(file_manager != nullptr, "create file manager failed!");

        auto config =
            milvus::index::ParseConfigFromIndexParams(info.index_params);
        config["index_files"] = info.index_files;

        info.index = milvus::index::IndexFactory::GetInstance().CreateIndex(
            index_info, file_manager);
        info.index->Load(config);

        segment->LoadIndex(info);
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }
    void
    debug_print(grpc::CallbackServerContext* context,
                const milvus::proto::segcore::SearchRequest* request) {
        std::ostringstream ss;
        ss << "[";
        for (auto& segmentid : request->segmentids()) {
            ss << " " << segmentid << ",";
        }
        ss << "]";

        std::ostringstream ss2;
        ss2 << "[";
        for (auto& m : context->client_metadata()) {
            ss2 << " " << m.first << ":" << m.second << ",";
        }
        ss2 << "]";
        LOG_SERVER_INFO_ << "SearchRequest: " << ss.str()
                         << ", meta: " << ss2.str();
    }

    grpc::ServerUnaryReactor*
    SearchSegments(grpc::CallbackServerContext* context,
                   const milvus::proto::segcore::SearchRequest* request,
                   milvus::proto::internal::SearchResults* response) override {
        debug_print(context, request);
        auto collection = cm_->Get(request->req().collectionid());
        auto expr = request->req().serialized_expr_plan();
        auto plan = milvus::query::CreateSearchPlanByExpr(
            *collection->GetSchema(), expr.data(), expr.size());
        auto groupStr = request->req().placeholder_group();
        auto group = milvus::query::ParsePlaceholderGroup(plan.get(), groupStr);

        std::vector<milvus::SearchResult*> results;
        for (auto segmentId : request->segmentids()) {
            auto seg = collection->Get(segmentId);
            auto result = seg->Search(
                plan.get(), group.get(), request->req().travel_timestamp());
            results.push_back(result.release());
        }
        std::array<int64_t, 1> nqs{request->req().nq()};
        std::array<int64_t, 1> topks{request->req().topk()};
        auto reducer = milvus::segcore::ReduceHelper(
            results, plan.get(), nqs.begin(), topks.begin(), 1);
        reducer.Reduce();
        reducer.Marshal();
        auto blobs = reducer.GetSearchResultDataBlobs();

        response->set_metric_type(request->req().metrictype());
        response->set_num_queries(request->req().nq());
        response->set_top_k(request->req().topk());
        response->set_sliced_blob(
            std::string{blobs->blobs[0].begin(), blobs->blobs[0].end()});
        response->set_sliced_offset(1);
        response->set_sliced_num_count(1);
        response->mutable_status()->set_code(milvus::proto::common::Success);

        std::for_each(results.begin(),
                      results.end(),
                      [](milvus::SearchResult* result) { delete result; });
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

    grpc::ServerUnaryReactor*
    Insert(grpc::CallbackServerContext* context,
           const milvus::proto::segcore::InsertRequest* request,
           milvus::proto::common::Status* response) override {
        auto sm = cm_->Get(request->collectionid());
        auto segment = static_cast<milvus::segcore::SegmentGrowing*>(
            sm->Get(request->segmentid()).get());
        auto num_rows = request->record().num_rows();
        auto offset = segment->PreInsert(num_rows);
        segment->Insert(offset,
                        num_rows,
                        request->rowids().data(),
                        request->timestamps().data(),
                        &request->record());
        response->set_code(milvus::proto::common::Success);
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

    grpc::ServerUnaryReactor*
    Delete(grpc::CallbackServerContext* context,
           const milvus::proto::segcore::DeleteRequest* request,
           milvus::proto::common::Status* response) override {
        auto sm = cm_->Get(request->collection_id());
        auto segment = sm->Get(request->segment_id());
        auto size = request->timestamps().size();
        auto status = segment->Delete(
            0, size, &request->primary_keys(), request->timestamps().data());
        if (status.ok()) {
            response->set_code(milvus::proto::common::Success);
        } else {
            response->set_code(milvus::proto::common::Failed);
        }
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

 private:
    std::shared_ptr<CollectionManager> cm_;
};

class LoggingInterceptor
    : public grpc::experimental::ServerInterceptorFactoryInterface {
 public:
    class Interceptor : public grpc::experimental::Interceptor {
     public:
        explicit Interceptor(grpc::experimental::ServerRpcInfo* info)
            : rpcInfo_(info) {
        }

        void
        Intercept(
            grpc::experimental::InterceptorBatchMethods* methods) override {
            try {
                LOG_SEGCORE_INFO_ << "Proceed RPC: " << rpcInfo_->method();
            } catch (std::exception& e) {
            }
            try {
                methods->Proceed();
            } catch (std::exception& e) {
                LOG_SERVER_WARNING_ << e.what();
                const boost::stacktrace::stacktrace* st =
                    boost::get_error_info<traced>(e);
                if (st) {
                    LOG_SERVER_WARNING_ << *st << '\n';
                }
            }
        }

     private:
        grpc::experimental::ServerRpcInfo* rpcInfo_;
    };

    grpc::experimental::Interceptor*
    CreateServerInterceptor(grpc::experimental::ServerRpcInfo* info) override {
        return new Interceptor(info);
    }
};

void
RunServer(const std::string& server_address) {
    LOG_SEGCORE_INFO_ << "Try to start SegcoreService ...";
    SegcoreService service(std::make_shared<CollectionManager>());

    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    builder.SetSyncServerOption(grpc::ServerBuilder::NUM_CQS, 4);
    builder.SetSyncServerOption(grpc::ServerBuilder::MAX_POLLERS, 4);
    // grpc::ResourceQuota quota;
    // quota.SetMaxThreads(50);
    // builder.SetResourceQuota(quota);

    // auto cq = builder.AddCompletionQueue();

    std::vector<
        std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>>
        creators;
    creators.push_back(std::make_unique<LoggingInterceptor>());

    builder.experimental().SetInterceptorCreators(std::move(creators));
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        LOG_SEGCORE_ERROR_ << "Failed to start Segcore, the address "
                           << server_address << " may be used";
        return;
    }

    LOG_SEGCORE_INFO_ << "Server listening on " << server_address;

    server->Wait();
}

// const char* backtraceFileName = "./backtraceFile.dump";
//
// void
// signalHandler(int) {
//     ::signal(SIGSEGV, SIG_DFL);
//     ::signal(SIGABRT, SIG_DFL);
//     std::cout << boost::stacktrace::stacktrace();
//     boost::stacktrace::safe_dump_to(backtraceFileName);
//     ::raise(SIGABRT);
// }

namespace po = boost::program_options;

void
InitStorageConfig(milvus::storage::StorageConfig& config) {
    milvus::storage::RemoteChunkManagerSingleton::GetInstance().Init(config);
}

int
main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    fLB::FLAGS_logtostderr = true;
    fLI::FLAGS_minloglevel = 0;

    // ::signal(SIGSEGV, signalHandler);
    // ::signal(SIGABRT, signalHandler);

    po::options_description desc("Options");
    desc.add_options()                      // Add options
        ("help,h", "produce help message")  // Add help option
        ("address,a",
         po::value<std::string>()->default_value("0.0.0.0:19532"),
         "address")  // listened port which default is 19532
        ("storage_address",
         po::value<std::string>()->default_value("localhost:9000"),
         "s3/minio address")  // s3/minio address
        ("storage_bucket",
         po::value<std::string>()->default_value("a-bucket"),
         "s3 storage_bucket")  // s3 storage_bucket
        ("storage_key_id",
         po::value<std::string>()->default_value("minioadmin"),
         "s3 access key")  // s3 access key
        ("storage_key_value",
         po::value<std::string>()->default_value("minioadmin"),
         "s3 secret key")  // s3 secret key
        ("storage_root_path",
         po::value<std::string>()->default_value("files"),
         "s3 root path")  // s3 root path
        ("storage_type",
         po::value<std::string>()->default_value("minio"),
         "storage type, one of minio,s3,gcp")  // storage type, one of minio,s3,gcp
        ("storage_iam_endpoint",
         po::value<std::string>()->default_value(""),
         "endpoint for iam")  // endpoint for iam
        ("storage_use_ssl",
         po::value<bool>()->default_value(false),
         "use ssl or not")  // use ssl or not
        ("storage_use_iam",
         po::value<bool>()->default_value(false),
         "use iam or not");  // use iam or not

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    milvus::storage::StorageConfig config{
        vm["storage_address"].as<std::string>(),
        vm["storage_bucket"].as<std::string>(),
        vm["storage_key_id"].as<std::string>(),
        vm["storage_key_value"].as<std::string>(),
        vm["storage_root_path"].as<std::string>(),
        vm["storage_type"].as<std::string>(),
        vm["storage_iam_endpoint"].as<std::string>(),
        vm["storage_use_ssl"].as<bool>(),
        vm["storage_use_iam"].as<bool>()};
    InitStorageConfig(config);

    RunServer(vm["address"].as<std::string>());
    return 0;
}
