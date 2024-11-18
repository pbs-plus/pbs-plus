//go:build linux

package partial_files

import (
	"encoding/json"
	"net/http"

	"github.com/sonroyaalmerol/pbs-plus/internal/proxy"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func D2DPartialFileHandler(storeInstance *store.Store) func(http.ResponseWriter, *http.Request, map[string]string) {
	return func(w http.ResponseWriter, r *http.Request, pathVar map[string]string) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		if err := storeInstance.CheckProxyAuth(r); err != nil {
			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
			return
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

			proxy.ExtractTokenFromRequest(r, storeInstance)

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(toReturn)

			return
		}
	}
}

func ExtJsPartialFileHandler(storeInstance *store.Store) func(http.ResponseWriter, *http.Request, map[string]string) {
	return func(w http.ResponseWriter, r *http.Request, pathVar map[string]string) {
		response := PartialFileConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		if err := storeInstance.CheckProxyAuth(r); err != nil {
			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		newPartialFile := store.PartialFile{
			Substring: r.FormValue("substring"),
			Comment:   r.FormValue("comment"),
		}

		err = storeInstance.CreatePartialFile(newPartialFile)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		proxy.ExtractTokenFromRequest(r, storeInstance)

		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
	}
}

func ExtJsPartialFileSingleHandler(storeInstance *store.Store) func(http.ResponseWriter, *http.Request, map[string]string) {
	return func(w http.ResponseWriter, r *http.Request, pathVar map[string]string) {
		response := PartialFileConfigResponse{}
		if r.Method != http.MethodPut && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		if err := storeInstance.CheckProxyAuth(r); err != nil {
			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPut {
			err := r.ParseForm()
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			partialFile, err := storeInstance.GetPartialFile(pathVar["partial_file"])
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			if r.FormValue("substring") != "" {
				partialFile.Substring = r.FormValue("substring")
			}
			if r.FormValue("comment") != "" {
				partialFile.Comment = r.FormValue("comment")
			}

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "substring":
						partialFile.Substring = ""
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

			proxy.ExtractTokenFromRequest(r, storeInstance)

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodGet {
			partial_file, err := storeInstance.GetPartialFile(pathVar["partial_file"])
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			proxy.ExtractTokenFromRequest(r, storeInstance)

			response.Status = http.StatusOK
			response.Success = true
			response.Data = partial_file
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodDelete {
			err := storeInstance.DeletePartialFile(pathVar["partial_file"])
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			proxy.ExtractTokenFromRequest(r, storeInstance)

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)
			return
		}
	}
}
