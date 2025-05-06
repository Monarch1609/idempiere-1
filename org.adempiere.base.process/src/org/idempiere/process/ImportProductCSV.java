/******************************************************************************
 * Product: iDempiere ERP & CRM Smart Business Solution                       *
 * Copyright (C) 1999-2023 ComPiere, Inc. All Rights Reserved.                *
 * This program is free software; you can redistribute it and/or modify it    *
 * under the terms version 2 of the GNU General Public License as published   *
 * by the Free Software Foundation. This program is distributed in the hope   *
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied *
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.           *
 * See the GNU General Public License for more details.                       *
 * You should have received a copy of the GNU General Public License along    *
 * with this program; if not, write to the Free Software Foundation, Inc.,    *
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.                     *
 *****************************************************************************/
package org.idempiere.process;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;
import java.util.logging.Level;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.*;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.*;
import org.compiere.process.DocAction;

/**
 * Processus pour importer des produits et leurs prix depuis des fichiers CSV
 */
@org.adempiere.base.annotation.Process
public class ImportProductCSV extends SvrProcess {

    private int m_AD_Client_ID = 11;
    private int m_AD_Org_ID = 0;
    private String m_productCSV = null;
    private String m_priceCSV = null;
    private int m_M_Product_Category_ID = 105;
    private Charset m_charset = StandardCharsets.UTF_8;
    private int m_M_Warehouse_ID = 103;

    protected void prepare() {
        ProcessInfoParameter[] para = getParameter();
        for (ProcessInfoParameter p : para) {
            String name = p.getParameterName();
            if (name == null) continue;
            
            switch (name) {
                case "AD_Client_ID":
                    m_AD_Client_ID = p.getParameterAsInt(); break;
                case "AD_Org_ID":
                    m_AD_Org_ID = p.getParameterAsInt(); break;
                case "ProductCSV":
                    m_productCSV = (String) p.getParameter(); break;
                case "PriceCSV":
                    m_priceCSV = (String) p.getParameter(); break;
                case "M_Product_Category_ID":
                    m_M_Product_Category_ID = p.getParameterAsInt(); break;
                case "M_Warehouse_ID":
                    m_M_Warehouse_ID = p.getParameterAsInt(); break;
                default:
                    log.log(Level.SEVERE, "Paramètre inconnu: " + name);
            }
        }

        if (m_AD_Client_ID == 0)
            m_AD_Client_ID = Env.getAD_Client_ID(getCtx());
            
        if (m_AD_Org_ID == 0)
            m_AD_Org_ID = Env.getAD_Org_ID(getCtx());
            
        // Si le M_Warehouse_ID n'est pas défini, essayer de récupérer l'entrepôt par défaut pour l'organisation
        if (m_M_Warehouse_ID <= 0) {
            m_M_Warehouse_ID = getDefaultWarehouseID(m_AD_Org_ID);
            if (m_M_Warehouse_ID <= 0) {
                // Message d'information pour le journal
                log.log(Level.INFO, "Aucun entrepôt par défaut trouvé pour l'organisation. Veuillez spécifier un entrepôt.");
            }
        }
    }

    /**
     * Récupère l'entrepôt par défaut pour une organisation donnée
     */
    private int getDefaultWarehouseID(int orgID) {
        /*return DB.getSQLValueEx(null, 
            "SELECT M_Warehouse_ID FROM M_Warehouse WHERE AD_Org_ID=? AND IsActive='Y'", 
            orgID);*/
    	return 103;
    }

    protected String doIt() throws Exception {
        if (m_productCSV == null || m_productCSV.trim().isEmpty())
            throw new AdempiereException("Chemin fichier produits requis");

        if (m_priceCSV == null || m_priceCSV.trim().isEmpty())
            throw new AdempiereException("Chemin fichier prix requis");

        if (m_M_Warehouse_ID <= 0)
            throw new AdempiereException("Entrepôt requis. Veuillez sélectionner un entrepôt valide.");

        if (m_M_Product_Category_ID <= 0)
            throw new AdempiereException("Catégorie de produit requise");

        // Vérification des fichiers
        File productFile = new File(m_productCSV);
        if (!productFile.exists() || !productFile.canRead())
            throw new AdempiereException("Fichier produits introuvable ou inaccessible: " + m_productCSV);
            
        File priceFile = new File(m_priceCSV);
        if (!priceFile.exists() || !priceFile.canRead())
            throw new AdempiereException("Fichier prix introuvable ou inaccessible: " + m_priceCSV);

        List<String[]> productRecords = readCSV(productFile);
        List<String[]> priceRecords = readCSV(priceFile);

        if (productRecords.isEmpty())
            throw new AdempiereException("Fichier produits vide");
            
        if (priceRecords.isEmpty())
            throw new AdempiereException("Fichier prix vide");

        validateHeaders(productRecords.get(0), priceRecords.get(0));

        String trxName = Trx.createTrxName("ImportProductCSV");
        Trx trx = Trx.get(trxName, true);

        int productsImported = 0;
        int pricesImported = 0;

        try {
            MWarehouse wh = MWarehouse.get(getCtx(), m_M_Warehouse_ID);
            if (wh == null)
                throw new AdempiereException("Entrepôt avec ID " + m_M_Warehouse_ID + " introuvable");
                
            MLocator locator = wh.getDefaultLocator();
            if (locator == null) {
                // Création d'un emplacement par défaut
                locator = new MLocator(getCtx(), 0, trxName);
                locator.setM_Warehouse_ID(wh.getM_Warehouse_ID());
                locator.setValue("Standard");
                locator.setX("0");
                locator.setY("0");
                locator.setZ("0");
                locator.setIsDefault(true);
                locator.setAD_Org_ID(wh.getAD_Org_ID());
                if (!locator.save())
                    throw new AdempiereException("Erreur création emplacement par défaut");
            }

            Map<String, Integer> productMap = processProducts(productRecords, locator, trxName);
            productsImported = productMap.size();
            
            pricesImported = processPrices(priceRecords, productMap, trxName);

            trx.commit();
            return Msg.getMsg(getCtx(), "ImportCSVSuccess") + ": " + productsImported + " produits et " + pricesImported + " prix importés.";
        } catch (Exception e) {
            trx.rollback();
            log.log(Level.SEVERE, "Erreur lors de l'import", e);
            throw new AdempiereException("Erreur lors de l'import : " + e.getMessage(), e);
        } finally {
            trx.close();
        }
    }

    private List<String[]> readCSV(File file) throws IOException {
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), m_charset))) {
            String line;
            while ((line = br.readLine()) != null) {
                String trimmedLine = line.trim();
                if (!trimmedLine.isEmpty()) {
                    rows.add(trimmedLine.split(","));
                }
            }
        } catch (IOException e) {
            log.log(Level.SEVERE, "Erreur lecture fichier CSV " + file.getName(), e);
            throw e;
        }
        return rows;
    }

    private void validateHeaders(String[] productHeaders, String[] priceHeaders) {
        if (productHeaders.length < 5 ||
            !productHeaders[0].equalsIgnoreCase("organization") ||
            !productHeaders[1].equalsIgnoreCase("name") ||
            !productHeaders[2].equalsIgnoreCase("product_type") ||
            !productHeaders[3].equalsIgnoreCase("ref") ||
            !productHeaders[4].equalsIgnoreCase("init_stock")) {
            throw new AdempiereException("Format CSV produits invalide. En-têtes attendus: organization,name,product_type,ref,init_stock");
        }

        if (priceHeaders.length < 6 ||
            !priceHeaders[0].equalsIgnoreCase("ref") ||
            !priceHeaders[1].equalsIgnoreCase("price_list") ||
            !priceHeaders[2].equalsIgnoreCase("active") ||
            !priceHeaders[3].equalsIgnoreCase("list_price") ||
            !priceHeaders[4].equalsIgnoreCase("standard_price") ||
            !priceHeaders[5].equalsIgnoreCase("limit_price")) {
            throw new AdempiereException("Format CSV prix invalide. En-têtes attendus: ref,price_list,active,list_price,standard_price,limit_price");
        }
    }

    private Map<String, Integer> processProducts(List<String[]> records, MLocator locator, String trxName) {
        Map<String, Integer> productMap = new HashMap<>();

        MInventory inventory = new MInventory(getCtx(), 0, trxName);
        inventory.setAD_Org_ID(locator.getAD_Org_ID()); // Utiliser l'organisation du locator plutôt que m_AD_Org_ID
        inventory.setDescription("Import CSV Produits " + new Timestamp(System.currentTimeMillis()));
        inventory.setMovementDate(new Timestamp(System.currentTimeMillis()));
        
        // Récupérer le type de document d'inventaire correct
        int docTypeId = DB.getSQLValueEx(trxName, 
            "SELECT C_DocType_ID FROM C_DocType WHERE DocBaseType='MMI' AND AD_Client_ID=? ORDER BY IsDefault DESC", 
            m_AD_Client_ID);
        
        if (docTypeId <= 0) {
            throw new AdempiereException("Type de document d'inventaire introuvable");
        }
        
        inventory.setC_DocType_ID(docTypeId);
        inventory.setM_Warehouse_ID(locator.getM_Warehouse_ID());
        
        // Ajouter une validation supplémentaire
        if (!inventory.save()) {
            String error = CLogger.retrieveErrorString("Erreur");
            // Vérifier spécifiquement les problèmes d'organisation
            if (error.contains("Organization")) {
                throw new AdempiereException("Problème avec l'organisation. " +
                    "Vérifiez que l'organisation ID " + locator.getAD_Org_ID() + 
                    " existe et que l'utilisateur a les droits nécessaires.");
            } else {
                throw new AdempiereException("Erreur création inventaire: " + error);
            }
        }  
        // Récupérer les types de produits valides
        Set<String> validProductTypes = new HashSet<>();
        validProductTypes.add("I"); // Item
        validProductTypes.add("S"); // Service
        validProductTypes.add("E"); // Expense
        validProductTypes.add("R"); // Resource
        
        for (int i = 1; i < records.size(); i++) {
            String[] r = records.get(i);
            if (r.length < 5) {
                log.warning("Ligne " + i + " ignorée: données insuffisantes");
                continue;
            }

            try {
                int orgId = getOrCreateOrg(r[0], trxName);
                String name = r[1].trim();
                String type = r[2].trim();
                String ref = r[3].trim();
                
                if (name.isEmpty() || ref.isEmpty()) {
                    log.warning("Ligne " + i + " ignorée: nom ou référence vide");
                    continue;
                }
                
                BigDecimal stock;
                try {
                    stock = new BigDecimal(r[4].trim());
                } catch (NumberFormatException e) {
                    log.warning("Ligne " + i + ": stock invalide, utilisation de 0");
                    stock = BigDecimal.ZERO;
                }

                String productType;
                if (type.equalsIgnoreCase("item")) 
                    productType = "I";
                else if (type.equalsIgnoreCase("service"))
                    productType = "S";
                else if (type.equalsIgnoreCase("expense"))
                    productType = "E";
                else if (type.equalsIgnoreCase("resource"))
                    productType = "R";
                else {
                    log.warning("Ligne " + i + ": type produit invalide '" + type + "', utilisation de 'Item'");
                    productType = "I"; // Par défaut
                }

                // Vérifier si le produit existe déjà
                MProduct existingProduct = new Query(getCtx(), MProduct.Table_Name, "Value=?", trxName)
                    .setParameters(ref)
                    .first();
                
                MProduct product;
                if (existingProduct != null) {
                    product = existingProduct;
                    log.info("Produit existant mis à jour: " + ref);
                } else {
                    product = new MProduct(getCtx(), 0, trxName);
                    product.setValue(ref);
                }
                
                product.setAD_Org_ID(orgId);
                product.setName(name);
                product.setProductType(productType);
                product.setM_Product_Category_ID(m_M_Product_Category_ID);
                
                if (product.getC_UOM_ID() <= 0)
                    product.setC_UOM_ID(getDefaultUOM(trxName));
                    
                if (product.getC_TaxCategory_ID() <= 0)
                    product.setC_TaxCategory_ID(getDefaultTaxCategory(trxName));
                    
                product.setIsStocked("I".equals(productType));
                product.setIsSold(true);
                product.setIsPurchased(true);

                if (!product.save()) {
                    log.warning("Erreur création/mise à jour produit " + name + ": " + CLogger.retrieveErrorString("Erreur"));
                    continue;
                }

                int productId = product.getM_Product_ID();
                productMap.put(ref, productId);

                if (stock.compareTo(BigDecimal.ZERO) != 0) {
                    createInventoryLine(inventory, locator, productId, stock, trxName);
                }
            } catch (Exception e) {
                log.log(Level.WARNING, "Erreur traitement ligne " + i, e);
                // Continuer avec la ligne suivante
            }
        }
        
        // Compléter l'inventaire si des lignes ont été créées
        if (inventory.getLines(true).length > 0) {
            if (!inventory.processIt(DocAction.ACTION_Complete))
                log.warning("Erreur lors de la complétion de l'inventaire: " + inventory.getProcessMsg());
            
            inventory.saveEx();
        } else {
            // Supprimer l'inventaire vide
            inventory.deleteEx(true);
        }

        return productMap;
    }

    private int processPrices(List<String[]> records, Map<String, Integer> productMap, String trxName) {
        int count = 0;
        
        for (int i = 1; i < records.size(); i++) {
            String[] r = records.get(i);
            if (r.length < 6) {
                log.warning("Ligne prix " + i + " ignorée: données insuffisantes");
                continue;
            }

            try {
                String ref = r[0].trim();
                String priceListName = r[1].trim();
                boolean active = "Y".equalsIgnoreCase(r[2].trim()) || "true".equalsIgnoreCase(r[2].trim());
                
                BigDecimal list, std, limit;
                try {
                    list = new BigDecimal(r[3].trim());
                    std = new BigDecimal(r[4].trim());
                    limit = new BigDecimal(r[5].trim());
                } catch (NumberFormatException e) {
                    log.warning("Ligne prix " + i + " ignorée: valeurs numériques invalides");
                    continue;
                }

                Integer productId = productMap.get(ref);
                if (productId == null) {
                    log.warning("Produit introuvable pour ref: " + ref);
                    continue;
                }

                MPriceList pl = getOrCreatePriceList(priceListName, trxName);
                MPriceListVersion plv = getOrCreatePriceListVersion(pl, trxName);

                // Vérifier si le prix existe déjà
                MProductPrice existingPrice = new Query(getCtx(), MProductPrice.Table_Name, 
                    "M_PriceList_Version_ID=? AND M_Product_ID=?", trxName)
                    .setParameters(plv.getM_PriceList_Version_ID(), productId)
                    .first();
                
                MProductPrice pp;
                if (existingPrice != null) {
                    pp = existingPrice;
                } else {
                    pp = new MProductPrice(plv, productId, list, std, limit);
                }
                
                pp.setPriceLimit(limit);
                pp.setPriceList(list);
                pp.setPriceStd(std);
                pp.setIsActive(active);
                
                if (!pp.save()) {
                    log.warning("Erreur création/mise à jour prix pour produit " + ref + ": " + CLogger.retrieveErrorString("Erreur"));
                    continue;
                }
                
                count++;
            } catch (Exception e) {
                log.log(Level.WARNING, "Erreur traitement ligne prix " + i, e);
                // Continuer avec la ligne suivante
            }
        }
        
        return count;
    }

    private int getOrCreateOrg(String name, String trxName) {
        name = name.trim();
        if (name.isEmpty() || "*".equals(name))
            return m_AD_Org_ID; // Organisation par défaut
            
        int id = DB.getSQLValueEx(trxName, "SELECT AD_Org_ID FROM AD_Org WHERE Name=? AND IsActive='Y'", name);
        if (id > 0) return id;

        MOrg org = new MOrg(getCtx(), 0, trxName);
        org.setName(name);
        org.setValue(name);
        org.setIsActive(true);
        if (!org.save()) 
            throw new AdempiereException("Erreur création organisation " + name + ": " + CLogger.retrieveErrorString("Erreur"));
            
        return org.getAD_Org_ID();
    }

    private void createInventoryLine(MInventory inventory, MLocator locator, int productId, BigDecimal qty, String trxName) {
        MInventoryLine line = new MInventoryLine(getCtx(), 0, trxName);
        line.setM_Inventory_ID(inventory.getM_Inventory_ID());
        line.setM_Locator_ID(locator.getM_Locator_ID());
        line.setM_Product_ID(productId);
        line.setQtyBook(Env.ZERO);
        line.setQtyCount(qty);
        if (!line.save())
            throw new AdempiereException("Erreur ligne inventaire pour produit ID : " + productId + ": " + CLogger.retrieveErrorString("Erreur"));
    }

    private int getDefaultUOM(String trxName) {
        int uomId = DB.getSQLValueEx(trxName, 
            "SELECT C_UOM_ID FROM C_UOM WHERE IsDefault='Y' AND AD_Client_ID IN (0, ?)", m_AD_Client_ID);
            
        if (uomId <= 0) {
            // Si aucune UOM par défaut, utiliser "Each" (Unité)
            uomId = DB.getSQLValueEx(trxName, 
                "SELECT C_UOM_ID FROM C_UOM WHERE X12DE355='EA' AND AD_Client_ID IN (0, ?)", m_AD_Client_ID);
        }
        
        if (uomId <= 0)
            throw new AdempiereException("Unité de mesure par défaut introuvable");
            
        return uomId;
    }

    private int getDefaultTaxCategory(String trxName) {
        int taxCatId = DB.getSQLValueEx(trxName, 
            "SELECT C_TaxCategory_ID FROM C_TaxCategory WHERE IsDefault='Y' AND AD_Client_ID IN (0, ?)", m_AD_Client_ID);
            
        if (taxCatId <= 0) {
            // Si aucune catégorie de taxe par défaut, prendre la première
            taxCatId = DB.getSQLValueEx(trxName, 
                "SELECT C_TaxCategory_ID FROM C_TaxCategory WHERE AD_Client_ID IN (0, ?) AND IsActive='Y'", m_AD_Client_ID);
        }
        
        if (taxCatId <= 0)
            throw new AdempiereException("Catégorie de taxe par défaut introuvable");
            
        return taxCatId;
    }

    private MPriceList getOrCreatePriceList(String name, String trxName) {
        if (name == null || name.trim().isEmpty())
            throw new AdempiereException("Nom de liste de prix requis");
            
        name = name.trim();
        
        MPriceList pl = new Query(getCtx(), MPriceList.Table_Name, "Name=? AND AD_Client_ID=?", trxName)
            .setParameters(name, m_AD_Client_ID)
            .first();
            
        if (pl != null) return pl;

        // Récupérer la devise par défaut
        int currencyId = DB.getSQLValueEx(trxName, 
            "SELECT C_Currency_ID FROM C_Currency WHERE AD_Client_ID IN (0, ?)", m_AD_Client_ID);
            
        if (currencyId <= 0) {
            // EUR par défaut
            currencyId = 102; // EUR
        }

        pl = new MPriceList(getCtx(), 0, trxName);
        pl.setAD_Org_ID(m_AD_Org_ID);
        pl.setName(name);
        pl.setEnforcePriceLimit(false);
        pl.setIsDefault(false);
        pl.setIsSOPriceList(true);
        pl.setC_Currency_ID(currencyId);
        pl.setIsTaxIncluded(false);
        
        if (!pl.save()) 
            throw new AdempiereException("Erreur création liste prix " + name + ": " + CLogger.retrieveErrorString("Erreur"));
            
        return pl;
    }

    private MPriceListVersion getOrCreatePriceListVersion(MPriceList pl, String trxName) {
    List<MPriceListVersion> versions = new Query(getCtx(), MPriceListVersion.Table_Name, 
        "M_PriceList_ID=? AND AD_Client_ID=?", trxName)
        .setParameters(pl.getM_PriceList_ID(), m_AD_Client_ID)
        .setOrderBy("ValidFrom DESC")
        .list();

    if (!versions.isEmpty())
        return versions.get(0);

    // Récupérer le schéma de calcul de prix par défaut
    int schemaId = DB.getSQLValueEx(trxName, 
        "SELECT M_DiscountSchema_ID FROM M_DiscountSchema WHERE AD_Client_ID=? AND IsActive='Y'", 
        m_AD_Client_ID);
        
    if (schemaId <= 0) {
        // Prendre le premier schéma disponible
        schemaId = DB.getSQLValueEx(trxName, 
            "SELECT M_DiscountSchema_ID FROM M_DiscountSchema WHERE AD_Client_ID=? AND IsActive='Y'", 
            m_AD_Client_ID);
    }

    MPriceListVersion plv = new MPriceListVersion(getCtx(), 0, trxName);
    plv.setAD_Org_ID(m_AD_Org_ID);
    plv.setName("Version " + new Timestamp(System.currentTimeMillis()));
    plv.setDescription("Importé le " + new Timestamp(System.currentTimeMillis()));
    plv.setM_PriceList_ID(pl.getM_PriceList_ID());
    plv.setValidFrom(new Timestamp(System.currentTimeMillis()));
    
    if (schemaId > 0) {
        plv.setM_DiscountSchema_ID(schemaId);
    }
    
    if (!plv.save()) 
        throw new AdempiereException("Erreur création version prix: " + CLogger.retrieveErrorString("Erreur"));
        
    return plv;
}
}