//go:build linux

package partial_files

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func D2DPartialFileHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		if r.Method == http.MethodGet {
			all, err := storeInstance.GetAllPartialFiles()
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			digest, err := utils.CalculateDigest(all)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			toReturn := PartialFilesResponse{
				Data:   all,
				Digest: digest,
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(toReturn)

			return
		}
	}
}

func ExtJsPartialFileHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := PartialFileConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		newPartialFile := store.PartialFile{
			Path:    r.FormValue("path"),
			Comment: r.FormValue("comment"),
		}

		err = storeInstance.CreatePartialFile(newPartialFile)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
	}
}

func ExtJsPartialFileSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := PartialFileConfigResponse{}
		if r.Method != http.MethodPut && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPut {
			err := r.ParseForm()
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			pathDecoded, err := url.QueryUnescape(utils.DecodePath(r.PathValue("partial_file")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			partialFile, err := storeInstance.GetPartialFile(pathDecoded)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			if r.FormValue("path") != "" {
				partialFile.Path = r.FormValue("path")
			}
			if r.FormValue("comment") != "" {
				partialFile.Comment = r.FormValue("comment")
			}

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "path":
						partialFile.Path = ""
					case "comment":
						partialFile.Comment = ""
					}
				}
			}

			err = storeInstance.UpdatePartialFile(*partialFile)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodGet {
			pathDecoded, err := url.QueryUnescape(utils.DecodePath(r.PathValue("partial_file")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			partial_file, err := storeInstance.GetPartialFile(pathDecoded)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = partial_file
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodDelete {
			pathDecoded, err := url.QueryUnescape(utils.DecodePath(r.PathValue("partial_file")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			err = storeInstance.DeletePartialFile(pathDecoded)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)
			return
		}
	}
}
